# PM2.5 Parquet 档案读取计划 / 监测月报抽查名单服务

环境数据工程师在**按站点、日期分区的 PM2.5 Parquet 档案**中查找超标记录前，
先用本服务生成一份**可核对的文件读取计划**：服务只读 Parquet footer / 行组统计
（分区值、`min/max/null_count`），不扫描数据行，即给出

- 待读文件清单与**计划字节数**（保留文件字节求和）；
- 每个文件 **KEEP / EXCLUDE 的逐条原因**（中文说明 + 稳定原因码）；
- 档案**快照 ID**：清单替换后旧计划立即失效（HTTP 409）。

在清单模式下，服务还提供**监测月报抽查名单**：按站点 + 月份，对
“PM2.5 ≥ 35 μg/m³”和“缺测”两类记录分别做**种子固定的等概率无放回抽样**，
并且能随归档更正（清单换版）**只实读新增/换版文件**地增量更新名单，
更新结果与“按当前清单全量抽样”逐条一致；失败时保留上次成功名单，
重启后仍可查看。

技术栈：FastAPI + DuckDB + PyArrow，单页原生 HTML/JS 前端，Docker Compose 一键启动。

## 1. 目录结构

```
.
├── src/pm25_planner/
│   ├── config.py          # 环境变量配置
│   ├── models.py          # 请求/响应 Pydantic 模型
│   ├── parquet_meta.py    # PyArrow 元数据提取 + DuckDB 独立交叉核对
│   ├── snapshot.py        # 档案快照（目录扫描 / 清单 JSON 两种来源）+ LRU 缓存
│   ├── planner.py         # 逐文件保留/排除判定引擎（纯函数，含全部原因码）
│   ├── sampling.py        # 月报抽查名单：确定性抽签、增量更新、失败保留、持久化
│   ├── reader.py          # DuckDB 预览执行（同样绑定快照）
│   ├── api.py             # FastAPI 路由
│   └── main.py            # uvicorn 入口
├── web/index.html         # 单页前端（无构建步骤，含“读取计划/月报抽查”两个标签页）
├── scripts/
│   ├── generate_sample.py       # 生成本地样例档案（含各类异常文件；--manifest-only 只重建清单）
│   ├── sample_demo.py           # 抽样名单演示：生成 / 更正后更新 / 制造清单不符 / 恢复
│   ├── replace_manifest_demo.py # 清单原子替换 → 快照失效演示
│   └── entrypoint.sh            # 容器入口（按需自动造样例数据/补建清单）
├── tests/                 # 53 项 pytest 自测
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 2. 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

`docker-compose.yml` 默认就是**清单模式**（`PM25_MANIFEST_PATH=/data/manifest/manifest.json`），
状态目录为持久卷内的 `/data/state`。容器首次启动时档案为空卷，entrypoint 自动生成
13 个覆盖全部分支的样例 Parquet 文件与清单；之后重启不会重复生成（清单缺失时会按现有文件补建）。

在网页右上标签页切到“月报抽查名单”，选择站点与月份、填写两类名额和种子即可生成。

## 3. 本地开发运行

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

python scripts/generate_sample.py           # 生成 data/ 样例
PYTHONPATH=src uvicorn pm25_planner.main:app --port 8000
# http://localhost:8000
```

切换为**清单模式**（由外部系统维护档案清单，原子换文件）：

```bash
PM25_MANIFEST_PATH=$(pwd)/data/manifest/manifest.json \
PM25_STATE_DIR=$(pwd)/data/state \
  PYTHONPATH=src uvicorn pm25_planner.main:app --port 8000
```

配置项（均有默认值）：`PM25_ARCHIVE_DIR`、`PM25_MANIFEST_PATH`、`PM25_STATE_DIR`、
`PM25_COLUMN`（默认 `pm25`）、`PM25_PREVIEW_MAX_FILES`、`PM25_META_CACHE_SIZE`、
`PM25_SAMPLE_MAX_INCREMENTAL_FILES`（单次更新实读文件上限，默认 256）。

## 4. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/health` | 健康检查，含当前快照 ID |
| GET | `/api/snapshot` | 当前快照：ID、来源、文件数、总字节 |
| POST | `/api/plan` | 生成读取计划；带 `expected_snapshot_id` 时做快照绑定校验 |
| POST | `/api/plan/verify` | 核对某快照 ID 是否仍为当前快照 |
| POST | `/api/plan/preview` | 用 DuckDB 实读“保留文件”做小批量预览 |
| GET | `/api/debug/duckdb-stats?path=` | DuckDB 与 PyArrow 双引擎统计核对 |
| POST | `/api/samples` | 生成月报抽查名单并保存为“上次成功名单” |
| POST | `/api/samples/update` | 按原条件增量更新（只实读新增/换版文件） |
| POST | `/api/samples/audit` | 按当前清单全量重抽并逐条核对已保存名单 |
| GET | `/api/samples/last?station=&month=` | 查看上次成功名单、对应快照、最近一次尝试 |
| GET | `/api/samples` | 列出全部已保存名单的站点/月份 |

请求体字段（读取计划，全部可选）：

```json
{
  "stations": ["station-A"],
  "date_from": "2026-01-01",
  "date_to":   "2026-01-31",
  "pm25_min": 75.0,
  "pm25_max": null,
  "include_nulls": true,
  "expected_snapshot_id": "75c585af64ecfa17"
}
```

抽查名单请求体：

```json
{
  "station": "station-A",
  "month": "2026-01",
  "exceed_quota": 3,
  "missing_quota": 2,
  "exceed_seed": "2026-01-A-exceed",
  "missing_seed": "2026-01-A-missing"
}
```

清单已替换而请求携带旧 `expected_snapshot_id` 时返回：

```
HTTP 409
{"detail":{"error":"STALE_SNAPSHOT",
           "expected_snapshot_id":"...", "current_snapshot_id":"..."}}
```

## 5. 判定规则（与原因码一一对应）

判定只依据**分区值**与 **footer 统计**，顺序如下：

1. **文件缺失**（清单引用了磁盘上不存在的文件）→ `FILE_MISSING_KEEP` 保守保留；
2. **分区不完整/非法**（无法解析 station 或 date）→
   `PARTITION_UNPARSEABLE_KEEP`，跳过一切分区裁剪，且即使统计不相交也强制保留；
3. 分区完整时做站点 `PARTITION_STATION_EXCLUDED`、
   日期 `PARTITION_DATE_EXCLUDED` 裁剪；
4. footer 损坏 `FILE_UNREADABLE_KEEP`、缺列 `MISSING_COLUMN_KEEP`、
   无统计 `NO_STATS_KEEP` → 一律保守保留；
5. 统计完整时比较 min/max 与请求区间（**闭区间**，边界相等算相交）：
   相交 `STATS_MATCH_KEEP`，不相交 `STATS_RANGE_DISJOINT` 排除；
6. 缺测策略：
   - `include_nulls=true` 且文件有 null → `NULLS_INCLUDED_KEEP` 保留；
   - `include_nulls=false` 且文件只有 null/无区间内非空值 →
     `NULLS_PRESENT_BUT_EXCLUDED` 排除（分区不完整时仍保守保留）。

> 保守保留只会产生**误保留**（多读），不会漏读；前端与 `notes` 字段会显式提示。

## 6. 月报抽查名单

### 6.1 抽样口径

- 范围：某站点在某月（`YYYY-MM`）全部分区文件；
- 两类名额：**超标**（`pm25 ≥ 35`，恰好 35 计入）、**缺测**（`null`）；
  一行只归一类（null 不可能 ≥35），两类各有名额与种子；
- 两类分别**等概率无放回**抽取，不足名额则全取。

### 6.2 抽签顺序如何固定

每行的抽签键：

```
draw_key = HMAC-SHA256(seed, rel_path \0 size \0 mtime_ns \0 row_number)
```

`rel_path` / `size` / `mtime_ns` 即清单所记的定位信息，`row_number` 为文件内行号（从 1 起）。
把范围内全部合格行的抽签键升序排列、取前 `quota` 个，就是一次等概率无放回抽样。
名单按抽签键升序展示，每条给出：读数、文件相对路径、清单所记大小与修改时间、文件内行号。
（`mtime_ns` 超出 JS 安全整数范围，API 与页面按十进制字符串展示。）

### 6.3 增量更新与全量一致性

单个文件的抽签键只由该文件自身的 `(seed, path, size, mtime_ns)` 和内容决定，因此：

1. 文件未变（清单三要素相同且磁盘核对一致）→ 直接复用上次名单中该文件的候选，**不读文件**；
2. 新增/换版文件（任一要素变化）→ 按新身份重新抽签，每文件每类最多保留 `quota` 个候选；
3. 合并后在候选里取抽签键最小的 `quota` 个。

这与“按当前清单全量抽样”的键集合与取值规则完全相同，结果逐条一致；
新增文件带来更小的抽签键时会**挤掉仍有效的旧样本**，文件被撤回则其候选整体消失。
`/api/samples/update` 的响应里 `diff.added` / `diff.withdrawn` 标出相对上次成功名单的变化；
`/api/samples/audit` 会真的全量重抽一遍并逐条比对，供独立核对。

### 6.4 失败语义（保留上次成功名单）

以下情况都会中止生成/更新，返回 HTTP 4xx/5xx + 明确原因码，**不写入任何半成品**：

| 原因码 | 场景 |
|---|---|
| `FILE_MANIFEST_MISMATCH` | 磁盘文件的 size/mtime 与清单不一致，或读取期间文件被替换 |
| `FILE_MISSING` | 清单引用的文件不存在 |
| `FILE_UNREADABLE` | Parquet 损坏、缺列或读取失败 |
| `MANIFEST_CHANGED` | 操作期间清单再次换版（读取前后指纹不一致），结果可能混合两版 |
| `MANIFEST_INVALID` | 清单 JSON 损坏、路径越界/重复、字段非法 |
| `MANIFEST_UNAVAILABLE` | 清单文件缺失 |
| `SAMPLE_SPEC_CHANGED` | 更新参数与上次成功名单不同 |
| `INCREMENTAL_LIMIT_EXCEEDED` | 新增/换版文件超过单次实读上限 |
| `SAMPLE_STATE_CORRUPT` | 已保存的状态文件损坏 |

失败响应会带上 `last_success_snapshot_id` / `last_success_generated_at`。
每次成功名单与对应快照证据（清单版本、范围内逐文件定位信息）都原子写入
`PM25_STATE_DIR/<站点>__<月份>/last_success.json`，最近一次尝试写 `last_attempt.json`，
最近一次差异写 `last_diff.json`；**服务重启后** `/api/samples/last` 与网页仍可查看。

### 6.5 演示：换版后更新名单

```bash
docker compose up --build -d

# 1) 生成 2026-01 的名单（station-A，超标 3 / 缺测 2）
docker compose exec pm25-planner python scripts/sample_demo.py generate

# 2) 模拟归档更正：新增一个文件并原子换版清单，再增量更新
docker compose exec pm25-planner python scripts/sample_demo.py correct
#    输出本次实读文件、新增/撤回样本，并自动做全量重抽核对

# 3) 演示失败留痕：把清单里某文件 size 改错 → 更新返回 409，上次名单仍可查看
docker compose exec pm25-planner python scripts/sample_demo.py break-manifest

# 4) 恢复清单（按磁盘实际 size/mtime 修正）后可继续更新
docker compose exec pm25-planner python scripts/sample_demo.py restore
```

网页演示路径：标签页“月报抽查名单” → 点“生成名单” → 宿主机执行
`docker compose exec pm25-planner python scripts/sample_demo.py correct` →
回网页点“按原条件更新”查看新增/撤回框；点“全量重抽核对”验证逐条一致。

## 7. 快照与清单原子替换

- 快照 ID = 全部 `(相对路径, size, mtime_ns)` 有序集合的 SHA-256 前 16 位；
- 扫描模式按目录 mtime + 文件签名判断变化；清单模式把清单文件本身的
  size/mtime/version 一并纳入指纹；
- 清单写入采用“写临时文件 + `os.replace`”，读者只会看到旧版或新版完整 JSON，
  绝不会读到半截文件；读取期间被替换会显式报 `MANIFEST_CHANGED`，不产生混合版本；
- 元数据按 `(size, mtime_ns)` 做 LRU 缓存，文件变动自动失效。

演示：

```bash
python scripts/replace_manifest_demo.py
# 输出替换前后两个不同的快照 ID，旧 ID 经 /api/plan/verify 判定失效
```

## 8. 样例档案覆盖的场景

| 文件 | 场景 |
|---|---|
| station-A 三天 | 全低于阈值 / 含缺测的超标日 / 多 row group 跨阈值 |
| station-B | 全低于阈值 / **全 null 列** / 跨阈值无 null |
| station-C | `write_statistics=False` 无统计 / 缺 pm25 列 / **截断损坏** / 非法分区孤儿目录 |
| station-D | min==max 边界值，验证闭区间包含 |

## 9. 自测

```bash
PYTHONPATH=src pytest -q
# 53 passed
```

覆盖：原有 25 项（分区裁剪、闭区间边界、null 策略、保守保留、快照失效、409/503、
预览绑定、双引擎统计一致、路径穿越拦截）+ 抽查名单 28 项：
抽样口径与确定性、阈值 35 边界、名额不足全取、逐文件小顶堆与暴力 top-k 一致、
均匀性统计检验、生成/更新/核对接口、只读新增/换版文件、新增挤掉旧样本、
换版/撤回/缺失/损坏/清单不符/清单换版中途/路径穿越/状态损坏/更新参数变化/
增量上限等失败路径、失败后上次名单保留、重启后恢复。

