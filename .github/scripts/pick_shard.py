#!/usr/bin/env python3
"""直选抽注分片(GA): 每个 runner 拉自己负责的一段"待抽注文本"清单
(追加料文本 + 已OCR追加图), 逐条交 opencode 提取【福彩3D 当期直选注】,
结果 POST /api/ga/pick -> MySQL fact_pick_img(按来源先清后插)。

runner 内 PICK_CONCURRENCY 个 opencode 窗口并行; 主模型失败按序轮询备用模型。
长文本(综合区常上万字)按行切片后并集, 避免头部截断漏当期。

环境变量: API_BASE API_TOKEN AID ISSUE START END SHARD SHARDS OUT_DIR
          PICK_MODELS(逗号分隔, 顺序轮询) PICK_CONCURRENCY FORCE
          LLM_TIMEOUT SHARD_DEADLINE_SEC
"""
import csv
import json
import os
import re
import signal
import subprocess
import time
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

API_BASE = os.environ["API_BASE"].rstrip("/")
API_TOKEN = os.environ["API_TOKEN"]
AID = int(os.environ.get("AID") or "0")
ISSUE = os.environ.get("ISSUE", "")
START = int(os.environ.get("START", "1"))
END = int(os.environ.get("END", "1000"))
SHARD = int(os.environ.get("SHARD", "0"))
SHARDS = int(os.environ.get("SHARDS", "5"))
OUT_DIR = Path(os.environ.get("OUT_DIR", "out"))
FORCE = os.environ.get("FORCE", "0") == "1"
# 顺序轮询(首个成功即用), 非投票
MODELS = [m.strip() for m in os.environ.get(
    "PICK_MODELS",
    "opencode/space-bunny-free,opencode/big-pickle,opencode/mimo-v2.6-flash-free"
).split(",") if m.strip()]
CONC = int(os.environ.get("PICK_CONCURRENCY", "3"))
OPENCODE_BIN = os.environ.get("OPENCODE_BIN", "opencode")
LLM_TIMEOUT = int(os.environ.get("LLM_TIMEOUT", "120"))
_DL = int(os.environ.get("SHARD_DEADLINE_SEC", "0"))
DEADLINE = (time.time() + _DL) if _DL > 0 else 0

SYS = ("你是数据提取器。只输出JSON,不要调用任何工具,不要解释,"
       "不要查看文件。")

PROMPT = """下面是彩票推荐资料的文字内容(可能是OCR识别文本, 可能有错字)。
提取【福彩3D第{issue3}期】的直选注号码(三位数),规则:
- 只收"单挑/精选/精挑/直组/单组/直选/防守/挑战/惊天/猛干/推荐N注"等注列表里的三位数
- 注意标签可能在号码前面也可能在号码后面(如"026 037 福直组5倍"也算)
- 排除:复式码组(四五六七八码)、胆码、定位(百/十/个位)、组选/组六/组三、和值跨度、双飞两码
- 排除其他期号的内容(文本里可能含历史多期,只看{issue3}期)
- 排除排列三/排列五/体彩栏目的内容(只取福彩3D/福栏)
- 无分隔连续数字串按每3位拆一注
- 无符合内容则输出 {{"picks":[]}}
输出格式: {{"picks":["123","456"]}}

内容:
{text}"""


def api_get(path):
    req = urllib.request.Request(API_BASE + path,
                                 headers={"X-GA-Token": API_TOKEN})
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.load(r)


def api_post(path, obj):
    req = urllib.request.Request(
        API_BASE + path, data=json.dumps(obj).encode(),
        headers={"X-GA-Token": API_TOKEN, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return json.load(r)


def _run_llm(cmd, timeout):
    """运行 opencode; 超时杀整个进程组(含孙进程), 避免管道被占永久挂起。"""
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


def parse_picks(out):
    m = re.search(r'\{[^{}]*"picks"\s*:\s*\[.*?\][^{}]*\}', out or "", re.S)
    if not m:
        return None
    try:
        return [p for p in json.loads(m.group(0))["picks"]
                if re.fullmatch(r"\d{3}", str(p))]
    except Exception:
        return None


def _one(text, issue3):
    prompt = SYS + "\n" + PROMPT.format(issue3=issue3, text=text)
    for m in MODELS:
        try:
            picks = parse_picks(_run_llm(
                [OPENCODE_BIN, "run", "-m", m, "--title", "x", prompt],
                LLM_TIMEOUT))
        except Exception:
            picks = None
        if picks is not None:
            return picks, "llm-oc-" + m.rsplit("/", 1)[-1]
    return [], "fail"


def extract(text, issue3):
    """返回 (picks, via)。长文本按行切片后并集。"""
    text = (text or "").strip()
    if not text:
        return [], "none"
    if len(text) <= 3800:
        return _one(text, issue3)
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > 3500:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    if cur.strip():
        chunks.append(cur)
    picks, via = set(), "none"
    for ch in chunks:
        p, v = _one(ch, issue3)
        if v == "fail":
            return [], "fail"
        picks.update(p)
        via = v
    return sorted(picks), via


def work(item):
    kind = item["kind"]
    issue = item["issue"]
    issue3 = issue[-3:]
    if DEADLINE and time.time() > DEADLINE:
        return {"item": item, "status": "skip:deadline", "picks": 0}
    t0 = time.time()
    picks, via = extract(item.get("text", ""), issue3)
    ok = via != "fail"
    if kind == "img":
        src = f"ocr_img:{via}"
    else:
        src = f"追加料文本#{item.get('seq', 0)}:{via}"
    payload = {"kind": kind, "ref_id": item["ref_id"],
               "article_id": item["article_id"], "issue": issue,
               "sha256": item.get("sha256", ""), "seq": item.get("seq", 0),
               "src_line": src, "picks": picks, "ok": ok}
    resp = None
    if ok:
        for _ in range(3):
            try:
                resp = api_post("/api/ga/pick", {"items": [payload]})
                break
            except Exception as e:
                resp = {"err": str(e)[:80]}
                time.sleep(1)
    return {"item": item, "status": ("ok" if ok else "llm-fail"),
            "via": via, "picks": len(picks), "sec": round(time.time() - t0, 1),
            "resp": resp}


def main():
    total = END - START + 1
    per = (total + SHARDS - 1) // SHARDS
    s = START + SHARD * per
    e = min(END, s + per - 1)
    if s > END:
        print(f"shard {SHARD} empty (range {s}-{e})")
        return
    q = f"/api/ga/pick_tasks?start={s}&end={e}&all={1 if FORCE else 0}"
    if AID:
        q += f"&aid={AID}"
    elif ISSUE:
        q += f"&issue={ISSUE}"
    else:
        q += "&recent=1"      # 无 aid/issue: 只处理最新一期
    d = api_get(q)
    items = d["items"]
    print(f"shard {SHARD}/{SHARDS} range {d['start']}-{d['end']} "
          f"total={d['total']} count={len(items)} models={MODELS} conc={CONC} "
          f"force={FORCE}", flush=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    rows = []
    with ThreadPoolExecutor(max_workers=CONC) as ex:
        for r in ex.map(work, items):
            rows.append(r)
            it = r["item"]
            print(f"  [{it['kind']}:{it.get('seq', it.get('sha256','')[:8])}] "
                  f"{r['status']:9s} via={r.get('via','-'):24s} "
                  f"picks={r['picks']} {r.get('sec','')}s resp={r.get('resp')}",
                  flush=True)
    with open(OUT_DIR / f"shard_{SHARD}.json", "w", encoding="utf-8") as f:
        summary = []
        for r in rows:
            d = {k: v for k, v in r.items() if k != "item"}
            d.update({"kind": r["item"]["kind"], "ref_id": r["item"]["ref_id"],
                      "article_id": r["item"]["article_id"]})
            summary.append(d)
        json.dump(summary, f, ensure_ascii=False, indent=1)
    st = Counter(r["status"] for r in rows)
    print(f"shard {SHARD} done: {len(rows)} 条, status={dict(st)}, "
          f"picks={sum(r['picks'] for r in rows)}, {time.time()-t0:.0f}s",
          flush=True)


if __name__ == "__main__":
    main()
