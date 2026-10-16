#!/usr/bin/env python3
"""GA worker: manifest.json -> 并发下载(CDN直连->API代理兜底) -> opencode
vision 提取文字行 -> POST /api/ga/result 回传。
单 worker 内多图并发(IMG_CONCURRENCY); 逐张回传保证断点不丢;
vision 双模型失败时回传 failed, 由服务端累计失败次数(毒丸封顶)。"""
import json
import os
import subprocess
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

API = os.environ["API_BASE"].rstrip("/")
TOK = os.environ["API_TOKEN"]
MAXN = int(os.environ.get("MAXN") or "0")
BUDGET = int(os.environ.get("BUDGET_SEC") or "1500")   # 提前退出, 未处理图留待下轮
IMG_CONC = max(1, int(os.environ.get("IMG_CONCURRENCY") or "3"))
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
report = {"total": len(items), "processed": 0, "ok": 0, "vision_fail": 0,
          "dl_fail": 0, "via": {"cdn": 0, "api": 0}, "per_img": []}
t_start = time.time()
_lock = threading.Lock()


def fetch_img(it, dest):
    for attempt in range(3):   # CDN 偶发死边缘: 重试3次再兜底 API
        try:
            req = urllib.request.Request(
                it["url"], headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = r.read()
            dest.write_bytes(data)
            return "cdn"
        except Exception:
            time.sleep(0.5 * (attempt + 1))
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


def post(item, lines, model, via, failed=False):
    body = json.dumps({
        "worker": os.environ.get("GITHUB_RUN_ID", ""),
        "items": [{"sha256": item["sha256"], "url": item["url"],
                   "lines": lines, "model": model, "via": via,
                   "failed": failed}],
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


def handle(idx, it):
    if time.time() - t_start > BUDGET:
        return None   # 超预算: 不处理, 保持 pending 留待下轮
    sha = it.get("sha256", "")
    raw = work / f"{idx:05d}-{sha[:8]}.jpg"
    t0 = time.time()
    try:
        via = fetch_img(it, raw)
    except Exception as e:
        with _lock:
            report["dl_fail"] += 1
            report["per_img"].append({"sha": sha[:10], "err": f"dl:{e}"})
        return True
    lines, model = vision(raw)
    if not lines:
        with _lock:
            report["vision_fail"] += 1
            report["per_img"].append({"sha": sha[:10], "err": "vision"})
        post(it, [], "fail", via, failed=True)
        return True
    posted = post(it, lines, model, via)
    with _lock:
        report["via"][via] = report["via"].get(via, 0) + 1
        if posted:
            report["ok"] += 1
        report["per_img"].append({
            "sha": sha[:10], "t": round(time.time() - t0, 1),
            "lines": len(lines), "model": model.split("/")[-1], "via": via})
    print(f"[{idx+1}] {sha[:10]} {len(lines)}lines "
          f"{time.time()-t0:.0f}s via={via}", flush=True)
    return True


futs = []
with ThreadPoolExecutor(max_workers=IMG_CONC) as ex:
    for idx, it in enumerate(items):
        if time.time() - t_start > BUDGET:
            print(f"budget {BUDGET}s reached at {idx}/{len(items)}; "
                  f"{len(items)-idx} left -> re-queued next run", flush=True)
            break
        futs.append(ex.submit(handle, idx, it))
    done = 0
    for f in as_completed(futs):
        if f.result():
            done += 1
report["processed"] = done

work.joinpath("report.json").write_text(json.dumps(report, ensure_ascii=False))
print(json.dumps({k: v for k, v in report.items() if k != "per_img"},
                 ensure_ascii=False))
