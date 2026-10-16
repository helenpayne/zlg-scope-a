# AGENTS.md — zlg-scope-a 工作指引

本仓库是 **zlg-scope 采集系统的 GitHub Actions 计算端**（不是数据/配置所在处）。
数据、库表、面板、API 服务都在 batam-1 主仓 `/home/sw/dev_root/zlg-scope`（MySQL `zlg_scope`）。
这里只放：**工作流 + 运行脚本 + 实验结果**。

---

## 1. 它做什么

从主仓暴露的 API 拉取任务 → 从 CDN 下载图片 → 用 `opencode` 免费大模型（视觉）提取文字/分类 → 把结果回传 API。
用作免费、可并行的算力，替代本机跑 OCR/大模型。

数据流：

```
主仓 ga_api (batam-1)               本仓库 (GitHub Actions)
  /api/ga/pending  ──── prepare 决定分片数
  /api/ga/tasks    ──── manifest.json（每个 shard 一份待处理图清单）
  CDN 图片 url      ──── 下载（CDN 直连 → 失败回退 /api/ga/img/{sha}）
  /api/ga/result   ←──── 回传文字行 / 失败标记
```

---

## 2. 关键约定（务必遵守）

- **鉴权**：所有 API 请求带请求头 `X-GA-Token: $API_TOKEN`。`API_BASE`、`API_TOKEN` 来自仓库 Secrets，**严禁**写进代码或提交到仓库。
- **仓库是 public**：任何提交（含 `experiments/` 里的图片与 CSV）都会公开，**不要提交密钥、隐私或无关大文件**。
- **免费模型、免登录**：`opencode` 用 `-m opencode/xxx-free`，无需凭证。视觉附带图片用 `opencode run -m <model> -f <图片路径> --title x <提示词>`。
- **模型选择（实测）**：
  - 视觉最佳：`opencode/mimo-v2.6-flash-free`；密集大图慢，可回退 `opencode/longcat-2.5-preview-free`。
  - 文本最快：`opencode/space-bunny-free`；备用 `opencode/big-pickle`。
  - **避免** `opencode/nemotron-3.5-lightning-free`（实测全部超时）与 `kilo/*`（频繁超时）。
- **保持文件名稳定**：工作流的**文件名即分发 URL**。例如可视化实验工作流文件名仍是 `ocr_experiment.yml`（显示名为 vision-experiment），改名会破坏用户收藏的触发地址。
- **预算/超时**：GitHub 单 job 上限见各工作流 `timeout-minutes`；`ga_worker.py` 用 `BUDGET_SEC`(默认 1500) 提前退出，**未处理的图留给下一轮，不要让它死等**。
- **失败要回传**：模型彻底失败时调用 `/api/ga/result` 且 `failed=true`，让主仓累计 `ocr_attempt` 并在 3 次后封顶（避免毒丸图反复调度）。
- **`experiments/` 是自动产物**：`vision_<aid>.csv` 是持久数据库（只增不删），`images/<aid>/` 每次运行刷新。**不要手工编辑**这些文件（会干扰 sha 去重与增量跳过）。
- **改动前先本地校验**，再 push：
  ```
  python3 -c "import ast; ast.parse(open('.github/scripts/xxx.py').read())"
  python3 -c "import yaml; yaml.safe_load(open('.github/workflows/xxx.yml'))"
  ```

---

## 3. 目录结构

```
.github/workflows/
  pipeline.yml          # 生产 OCR 流水线: prepare 定分片 -> work(矩阵) 并发识别
  ocr_experiment.yml    # 分片实验(显示名 vision-experiment): 图片直接交大模型分类
  smoke.yml             # 冒烟: 环境/CDN/图片/模型连通性探测
.github/scripts/
  ga_worker.py          # pipeline 的 worker: 并发下载 + 视觉提字 + 回传
  vision_shard.py       # 实验的 worker: 下载 + 图片直判 直选/色彩吧/垃圾 + 汇总
experiments/
  vision_<aid>.csv      # 该文章分类**数据库**(持久; 只追加/更新, 不删除)
  vision_<aid>_*.csv    # 早期按区间命名的结果(仍在, 会被一并读入作为历史)
  images/<aid>/         # 该文章本次运行下载的图片(每次运行清上期, 只留本次)
```

### 数据/图片存储与跳过机制（重要）

- **CSV 即数据库**：`experiments/vision_<aid>.csv` 持久保存该文章全部分类结果，**只追加/更新、从不删除**。`combine` 会先并入所有 `vision_<aid>*.csv`（含历史区间文件）再叠加本次结果，按 `sha256` 去重、按 `idx` 排序。
- **跳过已判（增量）**：worker 开始前读取上述 CSV 的 `sha256` 集合，**已在库中的图片直接跳过（不下载、不调用模型）**。只处理新增图片——这正是"追加更新"的含义。
- **整批垃圾来源跳过**：`experiments/<aid>.skip` 记录"同一上传秒(URL 里 14 位 ts)内 >=2 张且全为垃圾"的来源；命中该 ts 的图片**直接跳过（不下载、不调用模型）**。用于拦掉快乐8等整批垃圾海报。
- **`force=1`**：忽略数据库与跳过表、全部重跑（调试用）。
- **图片按文章分目录**：`experiments/images/<aid>/`；每次运行**先清空该目录再放入本次下载的图片**，避免仓库无限膨胀。历史扁平图片（`experiments/images/*.jpg`）为旧版遗留，未清理。
- **扩展性**：一切以 `aid` 为键。后续处理其它文章的图片时，复用同一工作流（传不同 `aid`）或另建工作流即可，互不干扰。

---

## 4. 常用 API（主仓 ga_api，需 `X-GA-Token`）

| 端点 | 用途 |
|---|---|
| `GET /api/ga/pending?issue=&aid=` | 待识别(append 且 excluded=0、ocr_status=0)图数 |
| `GET /api/ga/tasks?issue=&aid=&shard=i/N&limit=&all=` | 取某分片任务清单（写 `manifest.json`） |
| `GET /api/ga/article_images?aid=&start=&end=` | 按序号切片返回某文章 append 图的 URL（实验用） |
| `GET /api/ga/img/{sha256}` | 图片代理回源（CDN 下载失败时兜底） |
| `POST /api/ga/result` | 回传 `{worker, items:[{sha256,url,lines,model,via,failed}]}` |
| `GET /api/ga/stats?issue=&aid=` | 按 ocr_status 统计 |

字段/状态：`fact_image.ocr_status`：`0=pending 1=done 2=failed 9=skipped`；`kind='append'` 为"追加料图"。
实验分类结果 `kind`：`3D直选` / `P3直选` / `色彩吧` / `垃圾`（`junk=true`）；非垃圾行在 `content` 列存大模型**原样提取**的号码/内容。

---

## 5. 怎么运行

**UI**：Actions 标签页 → 选工作流 → 右上 **Run workflow**（需有写权限的账号）。
**CLI**：
```bash
gh workflow run pipeline.yml -f aid=2663478 -f shards=0 -f img_concurrency=3
gh workflow run ocr_experiment.yml -f aid=2663478 -f start=1 -f end=100 -f shards=5
```
**REST**（等价）：
```bash
curl -X POST -H "Authorization: token $GH_TOKEN" -H "Accept: application/vnd.github+json" \
  https://api.github.com/repos/helenpayne/zlg-scope-a/actions/workflows/pipeline.yml/dispatches \
  -d '{"ref":"main","inputs":{"aid":"2663478"}}'
```

`pipeline.yml` 的 `shards=0` 表示按 `pending` 自动定分片（每片 40 张，上限 16）；定时任务 `*/10 4-15 * * *`(UTC) 自动消化。

---

## 6. 改代码时怎么做

1. **先看现有脚本的写法**（环境变量名、回传格式、并发模式），保持一致。
2. 环境变量（`ga_worker.py`）：`API_BASE API_TOKEN MAXN BUDGET_SEC IMG_CONCURRENCY`；
   （`vision_shard.py`）：`API_BASE API_TOKEN AID START END SHARD SHARDS OUT_DIR VISION_MODELS VISION_CONCURRENCY`。
3. 视觉提示词要**只输出一行 JSON**，脚本用"找 `{`…`}` 并解析"的容错方式取值；同时提供关键词兜底。
4. 并发：worker 内用 `ThreadPoolExecutor`；注意 `opencode` 并发过高会触发限流——主模型失败自动换备用模型。
5. 在 `main` 上验证：push 后 dispatch 一次小范围（如 `start=1&end=10`）确认无误，再跑全量。
6. 不要改主仓 API 契约；如需新增端点，先在主仓 `ga_api.py` 实现并重启服务。

---

## 7. 注意事项 / 已知问题

- **OCR 会丢分隔符**：RapidOCR 常把"026 568 569"连成"026568569…"，纯文本规则会误判复式/开奖串。这也是现方案改为**图片直交大模型（视觉）**的原因。
- **图片文件名**：`idx_<url basename>`（已剥离扩展名，`ext_of()` 另加真实后缀），避免 `.jpg.jpg` 双后缀。
- **分类只认福彩3D**：快乐8(01-80 两位数)、双色球、大乐透等非福彩3D彩种应判 `垃圾`；排列三同为三位数，目前仍并入直选（待定）。
- **CDN 偶发 SSL 超时/死边缘**：下载已做重试 3 次 + API 兜底，勿移除重试。
- **不要**在 `experiments/` 之外新增生成物；不要提交 `work/`、`all/`、`manifest.json` 等运行中间产物。
