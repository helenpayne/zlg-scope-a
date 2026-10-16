#!/usr/bin/env python3
"""OCR 分片实验: 每个 runner 拉取自己负责的一段"追加料图"URL,
下载 -> RapidOCR -> 文本存文件 -> 大模型判"直选/色彩吧/垃圾" -> 写 CSV。

环境变量: API_BASE API_TOKEN AID START END SHARD SHARDS OUT_DIR LLM_MODEL OCR_WORKERS
"""
import csv
import json
import os
import re
import subprocess
import threading
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
LLM_MODEL = os.environ.get("LLM_MODEL", "opencode/space-bunny-free")
OCR_WORKERS = int(os.environ.get("OCR_WORKERS", "4"))
OPENCODE_BIN = os.environ.get("OPENCODE_BIN", "opencode")
LLM_TIMEOUT = int(os.environ.get("LLM_TIMEOUT", "120"))
DL_TIMEOUT = 15
DL_RETRY = 3

PROMPT = (
    "你是彩票预测图文本分类器。下面是图片的OCR文字, 请按规则分类。\n\n"
    "【直选】文本中出现\"三位数号码组合列表\", 即多个三位数(000-999)组成的号码池。"
    "注意: OCR 可能把空格/逗号漏掉, 号码会连成一串, 例如 \"026568569578579589678679689\" "
    "实际是 026 568 569 578 579 589 678 679 689。常见于 组选/组三/组六/大底/号码池/直选注 等。"
    "只要存在这种三位数号码池, 就判为 直选(junk=false)。\n"
    "【色彩吧】文本含\"色彩吧\"色彩号码内容 -> kind=\"色彩吧\", junk=false\n"
    "【垃圾】其它, 特别是:\n"
    "  - 开奖/复盘内容: \"福开:xxx\"、\"体开:xxx\"、\"第xxx期:号码\"的已开奖结果、双飞验证(中/未中)\n"
    "  - 杀号/胆码/复式/和值/跨度/漏号/定位/双飞 等不含\"三位数号码池\"的内容\n"
    "  - 纯文字营销或无关内容\n"
    "  -> kind=\"垃圾\", junk=true\n\n"
    '只输出一行JSON, 不要解释: {"kind":"直选|色彩吧|垃圾","junk":true/false,"reason":"简短"}'
)
CATEGORIES = {"直选", "色彩吧", "垃圾"}

# 归一化: 去掉数字之间的 空格/逗号/连字符(OCR常丢空格导致号码连成一串)
SEP_RE = re.compile(r"(?<=\d)[ \t,，、\-/]+(?=\d)")
RUN_RE = re.compile(r"\d{9,}")   # 连续数字串, 至少 3 个三位数


def triples(text):
    """检测三位数号码池。返回 (三位组合个数, 样例)。
    先把数字间分隔符去掉, 再找长度>=9 且能被3整除的连续数字串。"""
    t = SEP_RE.sub("", text or "")
    pools = [m.group(0) for m in RUN_RE.finditer(t)]
    pools = [p for p in pools if len(p) % 3 == 0]
    n = sum(len(p) // 3 for p in pools)
    sample = ""
    if pools:
        p = pools[0]
        sample = " ".join(p[i:i + 3] for i in range(0, min(len(p), 30), 3))
    return n, sample

_ocr = None
_ocr_lock = threading.Lock()


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
    # 兜底: 走 API 图片端点(DB 已有归档)
    try:
        req = urllib.request.Request(f"{API_BASE}/api/ga/img/{sha}",
                                     headers={"X-GA-Token": API_TOKEN})
        with urllib.request.urlopen(req, timeout=DL_TIMEOUT) as r:
            return r.read()
    except Exception as e:
        raise RuntimeError(f"dl fail: {last} / {e}")


def ocr_lines(data):
    global _ocr
    from rapidocr_onnxruntime import RapidOCR
    with _ocr_lock:
        if _ocr is None:
            _ocr = RapidOCR()
        res, _ = _ocr(data)
    return [r[1] for r in (res or [])]


def llm_kind(text, hint=""):
    try:
        r = subprocess.run(
            [OPENCODE_BIN, "run", "-m", LLM_MODEL, "--title", "x",
             PROMPT + hint + "\n\nOCR文字:\n" + text],
            capture_output=True, text=True, timeout=LLM_TIMEOUT)
        out = r.stdout or ""
        m = None
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("{"):
                try:
                    j = json.loads(line)
                    if j.get("kind") in CATEGORIES:
                        m = j
                        break
                except Exception:
                    pass
        if m is None:
            for k in CATEGORIES:
                if k in out:
                    return {"kind": k, "junk": k == "垃圾", "reason": "fallback"}
            return {"kind": "?", "junk": None, "reason": "parse fail"}
        return m
    except Exception as e:
        return {"kind": "?", "junk": None, "reason": f"llm err: {e}"}


def safe_name(idx, sha, url):
    base = url.split("?")[0].rstrip("/").split("/")[-1] or sha
    base = "".join(ch for ch in base if ch.isalnum() or ch in "._-")[:60]
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


def work(item):
    idx, sha, url = item["idx"], item["sha256"], item["url"]
    row = {"idx": idx, "sha256": sha, "url": url, "name": safe_name(idx, sha, url),
           "chars": 0, "triples": 0, "sample": "", "say": "", "kind": "?", "junk": "",
           "reason": "", "ocr_sec": "", "llm_sec": "", "status": "ok"}
    t0 = time.time()
    try:
        data = fetch_bytes(url, sha)
        lines = ocr_lines(data)
    except Exception as e:
        row["status"] = f"err:{e}"[:200]
        return row
    (OUT_DIR / "images").mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "images" / (row["name"] + ext_of(url, data))).write_bytes(data)
    text = "\n".join(lines)
    row["ocr_sec"] = round(time.time() - t0, 1)
    row["chars"] = len(text)
    ntri, sample = triples(text)
    row["triples"] = ntri
    row["sample"] = sample
    (OUT_DIR / "ocr").mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "ocr" / (row["name"] + ".txt")).write_text(text or "", encoding="utf-8")
    if not text.strip():
        row.update(kind="垃圾", junk="true", reason="empty ocr")
        return row
    t1 = time.time()
    hint = (f"\n\n[程序检测] 该文本中\"三位号码组合\"段数={ntri}"
            + (f", 样例: {sample}" if sample else ""))
    res = llm_kind(text, hint)
    row["llm_sec"] = round(time.time() - t1, 1)
    say = res.get("kind", "?")
    row["say"] = say
    if say == "色彩吧":
        final, junk = "色彩吧", False
    elif ntri > 0 or say == "直选":
        final, junk = "直选", False
    elif say in ("垃圾", "?"):
        final, junk = "垃圾", True
    else:
        final, junk = say, False
    row["kind"] = final
    row["junk"] = "true" if junk else "false"
    row["reason"] = (res.get("reason") or "")[:80]
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
    print(f"shard {SHARD}/{SHARDS} range {d['start']}-{d['end']} count={len(items)}", flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    rows = []
    with ThreadPoolExecutor(max_workers=OCR_WORKERS) as ex:
        for row in ex.map(work, items):
            rows.append(row)
            print(f"  [{row['idx']}] {row['kind']:4s} junk={row['junk']:5s} "
                  f"triples={row['triples']:3d} chars={row['chars']:5d} "
                  f"ocr={row['ocr_sec']}s llm={row['llm_sec']}s", flush=True)
    rows.sort(key=lambda r: r["idx"])
    csv_path = OUT_DIR / f"shard_{SHARD}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    j = sum(1 for r in rows if r["junk"] == "true")
    k = sum(1 for r in rows if r["kind"] == "色彩吧")
    print(f"shard {SHARD} done: {len(rows)} imgs, junk={j}, 色彩吧={k}, "
          f"直选={sum(1 for r in rows if r['kind']=='直选')}, {time.time()-t0:.0f}s "
          f"-> {csv_path}", flush=True)


if __name__ == "__main__":
    main()
