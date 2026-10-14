#!/usr/bin/env python3
"""GA worker: manifest.json -> 逐图下载(CDN直连->API代理兜底) -> 裁底50%
-> opencode vision 提取文字行 -> POST /api/ga/result 回传。
逐张回传保证断点不丢; 模型轮询分摊配额。"""
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

from PIL import Image

API = os.environ["API_BASE"].rstrip("/")
TOK = os.environ["API_TOKEN"]
MAXN = int(os.environ.get("MAXN") or "0")
MODELS = [
    "opencode/mimo-v2.6-flash-free",
    "opencode/longcat-2.5-preview-free",
]

PROMPT = ("读取这张彩票预测图,逐行提取图中全部文字行,每行原样输出到JSON数组。"
          '输出格式:{"lines":["...","..."]}。只输出JSON,不要解释,不要使用工具。')

manifest = json.load(open("manifest.json"))
items = manifest.get("items", [])
if MAXN:
    items = items[:MAXN]
work = Path("work")
work.mkdir(exist_ok=True)
report = {"total": len(items), "ok": 0, "vision_fail": 0, "dl_fail": 0,
          "via": {"cdn": 0, "api": 0}, "per_img": []}


def fetch_img(it, dest):
    try:
        req = urllib.request.Request(
            it["url"], headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            dest.write_bytes(r.read())
        return "cdn"
    except Exception:
        pass
    req = urllib.request.Request(
        f"{API}/api/ga/img/{it['sha256']}", headers={"X-GA-Token": TOK})
    with urllib.request.urlopen(req, timeout=40) as r:
        dest.write_bytes(r.read())
    return "api"


def vision(img_path):
    for m in MODELS:
        try:
            r = subprocess.run(
                ["opencode", "run", "-m", m, "-f", str(img_path),
                 "--title", "x", PROMPT],
                capture_output=True, text=True, timeout=150)
            out = r.stdout
            i, j = out.find("{"), out.rfind("}")
            if i >= 0 and j > i:
                d = json.loads(out[i:j + 1])
                if isinstance(d.get("lines"), list) and d["lines"]:
                    return [str(x) for x in d["lines"]], m
        except Exception:
            pass
    return None, "fail"


def post(item, lines, model, via):
    body = json.dumps({
        "worker": os.environ.get("GITHUB_RUN_ID", ""),
        "items": [{"sha256": item["sha256"], "url": item["url"],
                   "lines": lines, "model": model, "via": via}],
    }).encode()
    for _ in range(3):
        try:
            req = urllib.request.Request(
                f"{API}/api/ga/result", data=body,
                headers={"X-GA-Token": TOK,
                         "Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=30).read()
            return True
        except Exception:
            time.sleep(2)
    return False


for idx, it in enumerate(items):
    t0 = time.time()
    sha = it.get("sha256", "")
    raw = work / f"{sha[:10]}.jpg"
    crop = work / f"{sha[:10]}_b.jpg"
    try:
        via = fetch_img(it, raw)
    except Exception as e:
        report["dl_fail"] += 1
        report["per_img"].append({"sha": sha[:10], "err": f"dl:{e}"})
        continue
    try:
        im = Image.open(raw)
        w, h = im.size
        im.crop((0, h // 2, w, h)).save(crop, quality=88)
    except Exception as e:
        report["dl_fail"] += 1
        report["per_img"].append({"sha": sha[:10], "err": f"img:{e}"})
        continue
    lines, model = vision(crop)
    if not lines:
        report["vision_fail"] += 1
        report["per_img"].append({"sha": sha[:10], "err": "vision"})
        continue
    posted = post(it, lines, model, via)
    report["via"][via] = report["via"].get(via, 0) + 1
    if posted:
        report["ok"] += 1
    report["per_img"].append({"sha": sha[:10], "t": round(time.time() - t0, 1),
                              "lines": len(lines), "model": model.split("/")[-1],
                              "via": via})
    print(f"[{idx+1}/{len(items)}] {sha[:10]} {len(lines)}lines "
          f"{time.time()-t0:.0f}s via={via}", flush=True)

work.joinpath("report.json").write_text(json.dumps(report, ensure_ascii=False))
print(json.dumps({k: v for k, v in report.items() if k != "per_img"},
                 ensure_ascii=False))
