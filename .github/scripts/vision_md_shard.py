#!/usr/bin/env python3
"""实验: 把一篇文章的全部图片原样采集到仓库, 并逐图转写为 Markdown。

与 vision_shard.py 完全独立:
- 不读取 vision_*.csv / *.skip / junk_authors / author_whitelist 等任何历史黑名单;
- 不做直选/色彩吧/垃圾分类, 只做"整图文字转写";
- 每张图片都保留, 每张图片都产出一个 .md(成功或失败的占位记录);
- 仅使用本工作流自己的 manifest(experiments/vision_md_<aid>.csv)做断点续跑。

环境变量:
  API_BASE API_TOKEN AID START END SHARD SHARDS OUT_DIR
  MD_MODELS(逗号分隔, 依次回退) MD_CONCURRENCY FORCE
  LLM_TIMEOUT SHARD_DEADLINE_SEC
"""
import csv
import glob
import json
import os
import re
import signal
import subprocess
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

API_BASE = os.environ["API_BASE"].rstrip("/")
API_TOKEN = os.environ["API_TOKEN"]
AID = int(os.environ.get("AID", "2926371"))
START = int(os.environ.get("START", "1"))
END = int(os.environ.get("END", "1000"))
SHARD = int(os.environ.get("SHARD", "0"))
SHARDS = int(os.environ.get("SHARDS", "10"))
OUT_DIR = Path(os.environ.get("OUT_DIR", "out"))
MODELS = [m.strip() for m in os.environ.get(
    "MD_MODELS",
    "opencode/mimo-v2.6-flash-free,opencode/longcat-2.5-preview-free"
).split(",") if m.strip()]
CONC = int(os.environ.get("MD_CONCURRENCY", "5"))
FORCE = os.environ.get("FORCE", "0") == "1"
OPENCODE_BIN = os.environ.get("OPENCODE_BIN", "opencode")
LLM_TIMEOUT = int(os.environ.get("LLM_TIMEOUT", "240"))
DL_TIMEOUT = 20
DL_RETRY = 3
_DL = int(os.environ.get("SHARD_DEADLINE_SEC", "0"))
DEADLINE = (time.time() + _DL) if _DL > 0 else 0

DB = f"experiments/vision_md_{AID}.csv"
FIELDS = ["idx", "sha256", "url", "name", "ext", "status",
          "model", "sec", "chars", "md", "image"]

PROMPT = (
    "请把这张图片中的**全部可见文字**完整转写为 Markdown。\n"
    "要求:\n"
    "1. 逐字转写, 不要总结、不要翻译、不要改写、不要补全;\n"
    "2. 保持原有阅读顺序和层级: 标题用 #/##, 列表用 - 或数字列表, "
    "表格用 Markdown 表格, 多个独立区块之间空一行;\n"
    "3. 号码之间保留原图的分隔方式(空格/逗号/连字符), 不要自行合并或拆分;\n"
    "4. 看不清的单字写成 [无法辨认], 不要猜测;\n"
    "5. 忽略平台水印、二维码、纯装饰图形, 但保留海报标题、作者/栏目名、"
    "期号、玩法、号码、开奖号、日期等一切文字信息;\n"
    "6. 如果图中确实没有任何文字, 只输出: (无文字)\n"
    "只输出 Markdown 正文, 不要代码围栏, 不要前言或解释。"
)


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
    for attempt in range(DL_RETRY):
        try:
            req = urllib.request.Request(f"{API_BASE}/api/ga/img/{sha}",
                                         headers={"X-GA-Token": API_TOKEN})
            with urllib.request.urlopen(req, timeout=DL_TIMEOUT) as r:
                return r.read()
        except Exception as e:
            last = e
            time.sleep(1 + attempt)
    raise last


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


def _run_llm(cmd, timeout):
    """运行 opencode; 超时时杀掉整个进程组(含孙进程)。"""
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True, start_new_session=True)
    try:
        out, _ = p.communicate(timeout=timeout)
        return out
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass
        try:
            p.communicate(timeout=5)
        except Exception:
            pass
        raise


def clean_md(text):
    text = (text or "").replace("\r\n", "\n").strip()
    # 去掉模型偶尔包上的 ```markdown / ``` 围栏
    m = re.fullmatch(r"```(?:markdown|md)?\s*\n(.*?)\n```", text, re.S)
    if m:
        text = m.group(1).strip()
    return text


def transcribe(img_path):
    """按顺序尝试模型; 返回 (markdown, 使用模型, 状态)。"""
    errors = []
    for model in MODELS:
        try:
            out = _run_llm(
                [OPENCODE_BIN, "run", "-m", model, "-f", str(img_path),
                 "--title", "x", PROMPT], LLM_TIMEOUT)
            text = clean_md(out)
            if text:
                return text, model, "ok"
            errors.append(f"{model}:empty")
        except Exception as e:
            errors.append(f"{model}:{type(e).__name__}")
    return "", "-", "err:llm(" + ",".join(errors)[:160] + ")"


def load_done():
    done = set()
    if FORCE:
        return done
    for f in glob.glob(f"experiments/vision_md_{AID}*.csv"):
        try:
            with open(f, newline="", encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    if r.get("sha256") and (r.get("status") or "") == "ok":
                        done.add(f"{r.get('idx')}:{r['sha256']}")
        except Exception:
            pass
    return done


def write_md(path, item, text, model, status, sec, image_name):
    meta = [
        "---",
        f"aid: {AID}",
        f"idx: {item['idx']}",
        f"sha256: {item['sha256']}",
        "url: " + json.dumps(item.get("url") or "", ensure_ascii=False),
        "image: " + json.dumps(image_name, ensure_ascii=False),
        f"model: {model.split('/')[-1]}",
        f"status: {json.dumps(status, ensure_ascii=False)}",
        f"seconds: {sec}",
        "---",
        "",
    ]
    body = text or f"> 本图转写状态: {status}"
    path.write_text("\n".join(meta) + body.rstrip() + "\n", encoding="utf-8")


def work(item):
    idx, sha, url = item["idx"], item["sha256"], item.get("url") or ""
    name = safe_name(idx, sha, url)
    row = {"idx": idx, "sha256": sha, "url": url, "name": name, "ext": "",
           "status": "ok", "model": "", "sec": "", "chars": "0",
           "md": "", "image": ""}
    if DEADLINE and time.time() > DEADLINE:
        row["status"] = "skip:deadline"
        return row
    t0 = time.time()
    try:
        data = fetch_bytes(url, sha)
    except Exception as e:
        row["status"] = f"dl err:{e}"[:200]
        return row
    ext = ext_of(url, data)
    img_name = name + ext
    md_name = name + ".md"
    (OUT_DIR / "images").mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "md").mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "images" / img_name).write_bytes(data)
    text, model, status = transcribe(OUT_DIR / "images" / img_name)
    sec = round(time.time() - t0, 1)
    write_md(OUT_DIR / "md" / md_name, item, text, model, status, sec, img_name)
    row.update({"ext": ext, "status": status,
                "model": model.split("/")[-1] if model != "-" else "-",
                "sec": sec, "chars": str(len(text)),
                "md": f"md/{md_name}", "image": f"images/{img_name}"})
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
    items = d.get("items") or []
    print(f"shard {SHARD}/{SHARDS} range {d.get('start')}-{d.get('end')} "
          f"count={len(items)} models={MODELS} conc={CONC} force={FORCE}", flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    done = load_done()
    todo = [it for it in items
            if f"{it.get('idx')}:{it.get('sha256')}" not in done]
    print(f"shard {SHARD}: manifest 已完成 {len(items) - len(todo)} 张, "
          f"待转写 {len(todo)} 张(旧黑名单全部不生效)", flush=True)
    t0 = time.time()
    rows = []
    with ThreadPoolExecutor(max_workers=CONC) as ex:
        for row in ex.map(work, todo):
            rows.append(row)
            print(f"  [{row['idx']}] {row['status']:16s} "
                  f"chars={row['chars']:>5s} model={row['model']:22s} "
                  f"{row['sec']}s", flush=True)
    rows.sort(key=lambda r: int(r["idx"]))
    csv_path = OUT_DIR / f"shard_{SHARD}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    ok = sum(1 for r in rows if r["status"] == "ok")
    print(f"shard {SHARD} done: {len(rows)} 张, ok={ok}, "
          f"{time.time() - t0:.0f}s -> {csv_path}", flush=True)


if __name__ == "__main__":
    main()
