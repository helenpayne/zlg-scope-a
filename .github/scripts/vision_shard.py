#!/usr/bin/env python3
"""视觉分片实验: 每个 runner 拉自己负责的一段"追加料图"URL,
下载图片 -> 直接交给大模型(附图片)判断 直选/色彩吧/垃圾 -> 写 CSV。
runner 内 VISION_CONCURRENCY 个 opencode 窗口并行; 主模型受限/失败时回退备用模型。

环境变量: API_BASE API_TOKEN AID START END SHARD SHARDS OUT_DIR
          VISION_MODELS(逗号分隔) VISION_CONCURRENCY
"""
import csv
import glob
import json
import os
import subprocess
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

API_BASE = os.environ["API_BASE"].rstrip("/")
API_TOKEN = os.environ["API_TOKEN"]
AID = int(os.environ.get("AID", "2663478"))
START = int(os.environ.get("START", "1"))
END = int(os.environ.get("END", "100"))
SHARD = int(os.environ.get("SHARD", "0"))
SHARDS = int(os.environ.get("SHARDS", "5"))
OUT_DIR = Path(os.environ.get("OUT_DIR", "out"))
MODELS = [m.strip() for m in os.environ.get(
    "VISION_MODELS",
    "opencode/space-bunny-free,opencode/mimo-v2.6-flash-free").split(",") if m.strip()]
CONC = int(os.environ.get("VISION_CONCURRENCY", "5"))
FORCE = os.environ.get("FORCE", "0") == "1"
OPENCODE_BIN = os.environ.get("OPENCODE_BIN", "opencode")
LLM_TIMEOUT = int(os.environ.get("LLM_TIMEOUT", "240"))
DL_TIMEOUT = 15
DL_RETRY = 3

# CSV 数据库列(顺序固定; 空结果也按此写表头)
FIELDS = ["idx", "sha256", "url", "name", "kind", "junk", "reason",
          "model", "sec", "status"]

PROMPT = (
    "这是彩票预测图片。请根据图片内容判断类型:\n"
    "【直选】福彩3D 的三位数(000-999)号码组合列表/号码池: 三位数一组, 用空格/逗号/连字符分隔, "
    "常见于 组选/组三/组六/小底/大底/直选注/精选/单挑/直组 等号码池。\n"
    "【色彩吧】含色彩吧色彩号码内容。\n"
    "【垃圾】其它, 例如:\n"
    "  - 非福彩3D彩种: 快乐8(号码01-80两位数)、双色球(红蓝球)、大乐透、七星彩 等\n"
    "  - 开奖结果(福开/体开/第x期开奖号)、复式码(九码/八码/七码复式)\n"
    "  - 胆码/断组/分解/和值/跨度/漏号/定位/双飞 等非号码池内容、纯文字营销/无关内容\n"
    '只输出一行JSON, 不要解释: {"kind":"直选|色彩吧|垃圾","junk":true/false,"reason":"简短"}'
)
CATEGORIES = {"直选", "色彩吧", "垃圾"}


def load_known(aid):
    """读取已存在的 CSV 数据库(该文章所有历史结果)的 sha 集合, 用于跳过。"""
    known = set()
    for f in glob.glob(f"experiments/vision_{aid}*.csv"):
        try:
            with open(f, newline="", encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    if r.get("sha256"):
                        known.add(r["sha256"])
        except Exception:
            pass
    return known


def api_get(path):
    req = urllib.request.Request(API_BASE + path, headers={"X-GA-Token": API_TOKEN})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def fetch_bytes(url, sha):
    last = None
    for attempt in range(DL_RETRY):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=DL_TIMEOUT) as r:
                return r.read()
        except Exception as e:
            last = e
            time.sleep(1 + attempt)
    req = urllib.request.Request(f"{API_BASE}/api/ga/img/{sha}",
                                 headers={"X-GA-Token": API_TOKEN})
    with urllib.request.urlopen(req, timeout=DL_TIMEOUT) as r:
        return r.read()


def safe_name(idx, sha, url):
    base = url.split("?")[0].rstrip("/").split("/")[-1] or sha
    base = "".join(ch for ch in base if ch.isalnum() or ch in "._-")[:60]
    for e in (".jpeg", ".jpg", ".png", ".webp", ".gif"):
        if base.lower().endswith(e):
            base = base[:-len(e)]
            break
    return f"{idx:04d}_{base or sha[:12]}"


def ext_of(url, data):
    low = url.lower().split("?")[0]
    for e in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        if low.endswith(e):
            return e
    if data[:3] == b"\xff\xd8\xff":
        return ".jpg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if data[:4] == b"RIFF":
        return ".webp"
    return ".img"


def parse(out):
    for line in (out or "").splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                j = json.loads(line)
                if j.get("kind") in CATEGORIES:
                    return j
            except Exception:
                pass
    for k in CATEGORIES:
        if k in (out or ""):
            return {"kind": k, "junk": k == "垃圾", "reason": "fallback"}
    return None


def classify(img_path):
    """依次尝试各模型, 返回 (结果dict, 命中的模型)。"""
    last = "no model"
    for m in MODELS:
        try:
            r = subprocess.run(
                [OPENCODE_BIN, "run", "-m", m, "-f", str(img_path),
                 "--title", "x", PROMPT],
                capture_output=True, text=True, timeout=LLM_TIMEOUT)
            j = parse(r.stdout)
            if j:
                return j, m
            last = "parse fail"
        except Exception as e:
            last = str(e)
        time.sleep(0.5)
    return {"kind": "?", "junk": None, "reason": last[:80]}, "-"


def work(item):
    idx, sha, url = item["idx"], item["sha256"], item["url"]
    row = {"idx": idx, "sha256": sha, "url": url, "name": safe_name(idx, sha, url),
           "kind": "?", "junk": "", "reason": "", "model": "", "sec": "", "status": "ok"}
    t0 = time.time()
    try:
        data = fetch_bytes(url, sha)
    except Exception as e:
        row["status"] = f"dl err:{e}"[:200]
        return row
    (OUT_DIR / "images").mkdir(parents=True, exist_ok=True)
    img = OUT_DIR / "images" / (row["name"] + ext_of(url, data))
    img.write_bytes(data)
    res, model = classify(img)
    row["sec"] = round(time.time() - t0, 1)
    row["kind"] = res.get("kind", "?")
    row["junk"] = ("true" if res.get("kind") == "垃圾" else
                   ("false" if res.get("kind") in CATEGORIES else ""))
    row["reason"] = (res.get("reason") or "")[:80]
    row["model"] = model.split("/")[-1] if model != "-" else "-"
    return row


def main():
    total = END - START + 1
    per = (total + SHARDS - 1) // SHARDS
    s = START + SHARD * per
    e = min(END, s + per - 1)
    if s > END:
        print(f"shard {SHARD} empty (range {s}-{e})")
        return
    d = api_get(f"/api/ga/article_images?aid={AID}&start={s}&end={e}")
    items = d["items"]
    print(f"shard {SHARD}/{SHARDS} range {d['start']}-{d['end']} count={len(items)} "
          f"models={MODELS} conc={CONC} force={FORCE}", flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    known = set() if FORCE else load_known(AID)
    todo = [it for it in items if it["sha256"] not in known]
    print(f"shard {SHARD}: 跳过已判 {len(items)-len(todo)} 张, 待处理 {len(todo)} 张",
          flush=True)
    t0 = time.time()
    rows = []
    with ThreadPoolExecutor(max_workers=CONC) as ex:
        for row in ex.map(work, todo):
            rows.append(row)
            print(f"  [{row['idx']}] {row['kind']:4s} junk={row['junk']:5s} "
                  f"model={row['model']:22s} {row['sec']}s", flush=True)
    rows.sort(key=lambda r: r["idx"])
    csv_path = OUT_DIR / f"shard_{SHARD}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    from collections import Counter
    print(f"shard {SHARD} done: 处理 {len(rows)} 张, "
          f"kind={dict(Counter(r['kind'] for r in rows))}, "
          f"model={dict(Counter(r['model'] for r in rows))}, "
          f"{time.time()-t0:.0f}s -> {csv_path}", flush=True)


if __name__ == "__main__":
    main()
