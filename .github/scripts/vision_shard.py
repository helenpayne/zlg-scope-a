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
import re
import subprocess
import time
import urllib.request
from collections import Counter
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
# 每个元素 = 一票; 多票取多数(kind), 平票时列表靠后者优先(默认末位 mimo)。
# 同一模型可重复出现以获得多票(降低单次非确定性)。
MODELS = [m.strip() for m in os.environ.get(
    "VISION_MODELS",
    "opencode/space-bunny-free,opencode/space-bunny-free,opencode/mimo-v2.6-flash-free"
).split(",") if m.strip()]
CONC = int(os.environ.get("VISION_CONCURRENCY", "5"))
FORCE = os.environ.get("FORCE", "0") == "1"
OPENCODE_BIN = os.environ.get("OPENCODE_BIN", "opencode")
LLM_TIMEOUT = int(os.environ.get("LLM_TIMEOUT", "240"))
VOTE_RETRY = int(os.environ.get("VOTE_RETRY", "2"))
DL_TIMEOUT = 15
DL_RETRY = 3

# CSV 数据库列(顺序固定; 空结果也按此写表头)
FIELDS = ["idx", "sha256", "url", "name", "kind", "junk", "content",
          "author", "reason", "votes", "model", "sec", "status"]

PROMPT = (
    "背景: 这些是『福彩3D 资料图(综合参考区)』里的图片, 均属福彩3D 相关参考资料"
    "(也可能混入排列三/其它彩种)。请判断每张图的类型。\n"
    "【3D直选】福彩3D 的直选号码: 三位数(000-999), 号码每三个数一组, 用空格/逗号/连字符分隔, "
    "常见于 组选/组三/组六/小底/大底/精选/单挑/直组。\n"
    "【P3直选】排列三(排3/P3) 的三位数直选号码(本批可能混入)。\n"
    "【色彩吧】**只要图中出现『色彩吧』字样/栏目标签**, 无论内容是断组/杀码/色彩号码、往期开奖对照, "
    "还是完整三位直选注, **一律判 `色彩吧`**(绝不为垃圾, 也不判直选)。此规则优先级最高。\n"
    "【垃圾】非福彩3D/P3资料, 例如: 快乐8、双色球、大乐透、七星彩 等非3D/P3彩种; "
    "**仅列往期开奖号 / 历史战绩对照表**(逐行\"26x期 开346准\"式); "
    "**只有 胆码/定位/和值/跨度/复式/杀码 而没有任何成组的具体三位直选注**的图; 纯文字营销/无关内容。\n"
    "判定 3D直选/P3直选 的关键: 图中必须有**当期成组的具体三位数直选号码**(若干注, 如 258 069 028 …), "
    "或标注 直组/单挑/精选N注/组选N注/单式 的注。\n"
    "以下**不算直选, 判垃圾**: 『三胆159/双胆15/独胆5』等胆码、『百位0689 十位1458 个位1247』等定位范围、"
    "『和值14/跨度5』、『6码复式235679/八码复式…』『位选0156/04568』等整段复式; "
    "即使其中出现三位数字(如三胆159)也不算直选注。号码只是往期开奖记录同样判垃圾。\n"
    "另请识别图片的**作者/品牌标识名**(指海报**大标题/栏目/byline**, 如『飞龙在天』『孤舟』『双龙汇』『摇钱树』; "
    "图中若出现『色彩吧』等栏目标签, 请务必一并提取与其**同现的作者名**(即使整图内容是往期号码表); "
    "**忽略角落/边缘的固定平台水印**(如『臻料阁』等带'料阁/料图'的粉色标识), 那是平台不是作者; "
    "看不清或不明显则填空串)。\n"
    "若为 3D直选/P3直选/色彩吧, 在 content 字段**逐行原样提取图中全部文字**"
    "(含期号/玩法词/号码/开奖号/对错判定等), **行与行之间用换行分隔**, 保持原顺序, "
    "不要合并成一行、不要改写、不要漏行; 垃圾则 content 留空。\n"
    '只输出一行JSON, 不要解释: '
    '{"kind":"3D直选|P3直选|色彩吧|垃圾","junk":true/false,"content":"...","author":"作者名或空","reason":"简短"}'
)
CATEGORIES = {"3D直选", "P3直选", "色彩吧", "垃圾"}


def ts_of(url):
    m = re.search(r"/(\d{14})", url or "")
    return m.group(1) if m else ""


def load_known(aid):
    """读取已存在的 CSV 数据库(该文章所有历史结果)的 sha 集合, 用于跳过。"""
    known = set()
    for f in glob.glob(f"experiments/vision_{aid}*.csv"):
        try:
            with open(f, newline="", encoding="utf-8") as fh:
                for r in csv.DictReader(fh):
                    # 只把"成功(status=ok)"的行视为已判, 下载失败行下次重试
                    if r.get("sha256") and (r.get("status") or "").startswith("ok"):
                        known.add(r["sha256"])
        except Exception:
            pass
    return known


def load_skip_ts(aid):
    """读取"整批垃圾"的来源批次(URL上传秒)黑名单, 命中则整批跳过(不下载/不调模型)。"""
    out = set()
    for f in glob.glob(f"experiments/{aid}.skip*"):
        try:
            out |= {x.strip() for x in open(f, encoding="utf-8").read().split() if x.strip()}
        except Exception:
            pass
    return out


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


def parse(out):
    """从模型输出里找 JSON(容忍前后有其它文字, 且 content 可能含真实换行)。"""
    out = out or ""
    for line in out.splitlines():
        i, j = line.find("{"), line.rfind("}")
        if i >= 0 and j > i:
            try:
                k = json.loads(line[i:j + 1])
                if k.get("kind") in CATEGORIES:
                    return k
            except Exception:
                pass
    # content 带真实换行时 JSON 会跨多行: 按花括号配对提取
    i = out.find("{")
    while i >= 0:
        depth = 0
        for j in range(i, len(out)):
            if out[j] == "{":
                depth += 1
            elif out[j] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        k = json.loads(out[i:j + 1])
                        if k.get("kind") in CATEGORIES:
                            return k
                    except Exception:
                        pass
                    break
        i = out.find("{", i + 1)
    for k in ("3D直选", "P3直选", "色彩吧", "垃圾"):
        if k in out:
            return {"kind": k, "junk": k == "垃圾", "content": "", "reason": "fallback"}
    return None


def one_call(img_path, model):
    """单次调用; 失败/解析失败最多重试 VOTE_RETRY 次。返回结果dict或None。"""
    for _ in range(VOTE_RETRY):
        try:
            r = subprocess.run(
                [OPENCODE_BIN, "run", "-m", model, "-f", str(img_path),
                 "--title", "x", PROMPT],
                capture_output=True, text=True, timeout=LLM_TIMEOUT)
            j = parse(r.stdout)
            if j:
                return j
        except Exception:
            pass
        time.sleep(0.3)
    return None


def classify(img_path):
    """每个 MODELS 元素投一票, 对 kind 取多数(平票时列表靠后者优先)。
    返回 (结果dict, votes摘要, 命中模型)。"""
    results = [(m, one_call(img_path, m)) for m in MODELS]
    valid = [(m, j) for m, j in results if j and j.get("kind") in CATEGORIES]
    votes = "|".join(f"{m.split('/')[-1]}:{j['kind'] if j else 'X'}" for m, j in results)
    if not valid:
        return ({"kind": "?", "junk": None, "content": "", "reason": "all fail"},
                votes, "-")
    cnt = Counter(j["kind"] for _, j in valid)
    best = max(cnt.values())
    winners = [k for k, c in cnt.items() if c == best]
    rank = {m: i for i, m in enumerate(MODELS)}
    kind = winners[0]
    if len(winners) > 1:                       # 平票: 列表靠后者(优先级高)决定
        for m, j in reversed(valid):
            if j["kind"] in winners:
                kind = j["kind"]
                break
    cand = [(m, j) for m, j in valid if j["kind"] == kind] or valid
    m, j = max(cand, key=lambda x: rank.get(x[0], -1))
    j = dict(j)
    j["kind"] = kind
    j["junk"] = (kind == "垃圾")
    return j, votes, m


def work(item):
    idx, sha, url = item["idx"], item["sha256"], item["url"]
    row = {"idx": idx, "sha256": sha, "url": url, "name": safe_name(idx, sha, url),
           "kind": "?", "junk": "", "content": "", "author": "", "reason": "",
           "votes": "", "model": "", "sec": "", "status": "ok"}
    t0 = time.time()
    try:
        data = fetch_bytes(url, sha)
    except Exception as e:
        row["status"] = f"dl err:{e}"[:200]
        return row
    (OUT_DIR / "images").mkdir(parents=True, exist_ok=True)
    img = OUT_DIR / "images" / (row["name"] + ext_of(url, data))
    img.write_bytes(data)
    res, votes, model = classify(img)
    row["sec"] = round(time.time() - t0, 1)
    kind = res.get("kind", "?")
    author = " ".join(str(res.get("author") or "").split())[:40]
    row["author"] = author
    row["kind"] = kind
    row["junk"] = ("true" if kind == "垃圾" else
                   ("false" if kind in CATEGORIES else ""))
    row["content"] = "" if kind == "垃圾" else str(res.get("content") or "").strip()[:4000]
    row["reason"] = (res.get("reason") or "")[:80]
    row["votes"] = votes[:200]
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
    skip_ts = set() if FORCE else load_skip_ts(AID)
    todo, n_known, n_batch = [], 0, 0
    for it in items:
        if it["sha256"] in known:
            n_known += 1
        elif ts_of(it.get("url")) in skip_ts and skip_ts:
            n_batch += 1        # 整批垃圾来源, 直接跳过(不下载/不调模型)
        else:
            todo.append(it)
    print(f"shard {SHARD}: 跳过已判 {n_known} 张, 跳过整批垃圾 {n_batch} 张, "
          f"待处理 {len(todo)} 张", flush=True)
    t0 = time.time()
    rows = []
    with ThreadPoolExecutor(max_workers=CONC) as ex:
        for row in ex.map(work, todo):
            rows.append(row)
            print(f"  [{row['idx']}] {row['kind']:5s} junk={row['junk']:5s} "
                  f"author={row['author'][:12]:12s} model={row['model']:22s} {row['sec']}s "
                  f"votes={row['votes']}", flush=True)
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
