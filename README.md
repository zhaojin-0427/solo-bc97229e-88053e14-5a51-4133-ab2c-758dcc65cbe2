# LIMS 样本衍生谱系 API

基于 **FastAPI + SQLite（WAL）** 的样本亲本–衍生关系谱系服务。实验室数据工程师
可上传 LIMS 导出的 CSV 批次，服务保证合并后的完整谱系始终无环，并向研究人员
提供按样本号的全部直接/间接受影响样本追溯。

## 1. 业务规则

| 规则 | 实现方式 |
| --- | --- |
| CSV 两列：亲本样本号、衍生样本号，非空文本 | 上传后逐行校验，空字段/坏表头整批 `422` 拒收（兼容英文表头 `parent_sample`/`child_sample`） |
| 每行表示亲本参与产生衍生样本，允许多亲本 | `edges` 仅约束 `(parent, child)` 唯一，同一 child 可有任意多个 parent |
| 关系只增不改 | 仅提供写入与只读接口，无更新/删除端点 |
| 重复关系合一 | 库表 `UNIQUE(parent_id, child_id)` + 应用层去重，重复行计入 `duplicate_edges`，不重复入库 |
| 每批与**已入库**关系合并后必须无环；自环同样整批拒绝 | 内存中对合并图做三色迭代 DFS，检出环即整批 `409`，响应中返回一条真实环路 |
| 失败或重启不留半批，成功关系保留 | 每批一个 SQLite 事务，失败 `ROLLBACK`；WAL + 命名卷持久化，重启后已提交数据保留 |
| 一次追溯查询只读一个完整提交状态 | 追溯在单个延迟读事务内完成，SQLite 事务快照保证一致性 |
| 图校验不占库写事务，校验期间其他批次可提交、追溯保持响应 | 校验全程在 Python 内存进行、不持任何库锁；仅短写事务入库，WAL 使读写不互斥 |
| 上传等待写锁时，已入库数据的查询不被拖住 | 上传/提交/查询使用**彼此隔离的固定大小线程池**；写锁等待只占用提交池线程，查询池永不因上传占满而排队（见 §6） |
| 执行资源须有明确上限，不靠调大线程池/缩短锁等待/拒收合法上传 | `LINEAGE_UPLOAD_WORKERS` / `LINEAGE_COMMIT_WORKERS` / `LINEAGE_QUERY_WORKERS` / `LINEAGE_MAX_INFLIGHT_UPLOADS` 显式有界；超准入者异步排队而非拒收；`busy_timeout` 与重试上限保持原值 |
| 并发上传按提交时完整谱系判定；分别合法但合并成环的两批不能都成功；合法批次不能仅因别批提交被拒 | 乐观并发控制（OCC）：提交时校验图版本，冲突则回滚并基于最新完整谱系**库外重新校验后重试**，成环才拒收 |

## 2. 快速启动（Docker Compose）

```bash
docker compose up --build
```

启动后：

- 交互式 API 文档（Swagger UI，可直接演示上传/拒收/追溯）：<http://localhost:8000/docs>
- 零依赖内置演示页：<http://localhost:8000/demo>
- ReDoc 文档：<http://localhost:8000/redoc>
- 健康检查：<http://localhost:8000/api/v1/health>

SQLite 数据库位于命名卷 `lineage-data`（容器内 `/data`），容器重启/重建后
已提交关系保留。停止并清空数据：

```bash
docker compose down -v
```

## 3. 本地开发（无 Docker 时）

```bash
python3 -m venv --without-pip .venv
.venv/bin/python get-pip.py            # 若系统无 pip
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/uvicorn app.main:app --reload --port 8000
.venv/bin/pytest -q                    # 单元 / 接口 / 并发 / 执行资源隔离测试
```

## 4. HTTP 接口

### 4.1 上传批次 `POST /api/v1/batches`

`multipart/form-data`，字段名 `file`，UTF-8 CSV，表头示例：

```csv
亲本样本号,衍生样本号
S001,S002
S001,S003
S002,S004
```

成功响应：

```json
{
  "batch_id": 1,
  "received_rows": 3,
  "inserted_edges": 3,
  "duplicate_edges": 0,
  "graph_version": 1,
  "retry_count": 0
}
```

成环（含自环）响应 `409`，返回一条**真实存在的环**（首尾相同，相邻节点均为图中边）：

```json
{
  "error": "cycle_rejected",
  "detail": "批次被拒绝：合并后谱系存在环，已整批拒收",
  "context": { "cycle": ["S001", "S002", "S004", "S005", "S001"] }
}
```

自环时 `cycle` 形如 `["S020", "S020"]`。

### 4.2 追溯受影响样本 `GET /api/v1/samples/{sample_code}/affected`

返回该样本全部直接及间接受影响的衍生样本，**每个仅列一次、不含自身**；
响应携带所读取的 `graph_version`，即一次查询只对应一个完整提交状态：

```json
{
  "sample_code": "S001",
  "found": true,
  "graph_version": 3,
  "affected_samples": ["S002", "S003", "S004", "S010", "S011", "S012"],
  "affected_count": 6
}
```

### 4.3 只读视图

- `GET /api/v1/batches`：已提交批次清单（被拒批次不会出现）
- `GET /api/v1/graph`：当前完整谱系（版本号、样本数、边数、全部边）
- `GET /api/v1/health`：健康检查（容器 healthcheck 亦使用此端点）

## 5. curl 演示

```bash
curl -X POST http://localhost:8000/api/v1/batches \
  -F "file=@examples/batch1.csv;type=text/csv"
curl -X POST http://localhost:8000/api/v1/batches \
  -F "file=@examples/batch3-cycle.csv;type=text/csv"      # 409，返回环路
curl http://localhost:8000/api/v1/samples/S001/affected
```

`examples/` 目录另含多亲本、自环、重复关系等场景文件。

## 6. 并发正确性设计

```
上传批次
  │
  ├─ ① 快照读（BEGIN DEFERRED，WAL 不阻塞写）：取全部已提交边 + graph_version
  │
  ├─ ② 库外校验（不持任何库锁）：合并候选边 → 迭代 DFS 判环
  │        有环（含自环）→ 409 + 真实环路，立即结束
  │
  └─ ③ BEGIN IMMEDIATE 短写事务：
           ├─ 当前版本 != 快照版本（期间有别批提交）
           │     → ROLLBACK，回到 ① 用最新完整谱系重新校验后重试
           │       （合法批次因此不会仅因并发冲突被拒；重试上限可配置）
           └─ 版本一致 → 插入批次/样本/边，版本 +1，COMMIT
```

- 两个**分别合法、合并成环**的批次并发提交：先拿到写锁者提交并推进版本，
  后者重试时在最新谱系上检出环并 `409` —— 恰好一批成功（测试
  `tests/test_service.py::test_concurrent_batches_only_one_succeeds_when_merged_cycles`
  与 `tests/manual_concurrency.py` 重复验证）。
- 互不相关的合法批次并发：冲突仅触发重试，最终全部成功。
- 环检测复杂度 O(V+E)，迭代实现避免深谱系图的递归栈溢出。

### 6.1 执行资源与查询隔离

上传在写锁上等待时不能拖垮查询，靠的是**执行资源分组**而非调大线程池：

| 线程池 | 承担的阶段 | 默认线程数 | 关键点 |
| --- | --- | --- | --- |
| 上传池 | CSV 解析、快照读、库外判环 | `LINEAGE_UPLOAD_WORKERS`=4 | 从不持有写锁 |
| 提交池 | `BEGIN IMMEDIATE` 短写事务 | `LINEAGE_COMMIT_WORKERS`=1 | **写锁等待只发生在这里** |
| 查询池 | 追溯、批次列表、全图、健康检查 | `LINEAGE_QUERY_WORKERS`=4 | 与上传/提交不共享任何线程 |

- 每个池内的工作单元自行建立并在同一线程内关闭 SQLite 连接，连接不跨线程交接。
- 上传准入另有显式上限 `LINEAGE_MAX_INFLIGHT_UPLOADS`：超出者在事件循环中
  `await` 排队，不占工作线程、**不被拒收**；锁释放后仍按提交时谱系重判并提交。
- 因为 SQLite 只有一个写者，提交池仅需 1 个线程；上传的“等待”发生在提交池，
  查询池始终空闲可用。WAL 下读事务不被写事务阻塞，查询读到的始终是某个
  完整提交状态（含 `graph_version`）。
- 慢校验只占上传池线程，不阻塞提交池，因此其他批次仍能在校验进行时提交。
- 回归测试：`tests/test_resource_isolation.py` 覆盖“上传耗尽执行容量”“持锁时
  发起查询”“解锁后提交”“并发成环拒收”等交错场景。

## 7. 项目结构

```
app/
  config.py      # 环境变量配置（路径、超时、重试上限、上传上限）
  database.py    # 连接/WAL PRAGMA、建表、版本读取
  graph.py       # 图构建与环检测（迭代三色 DFS，含自环）
  csv_utils.py   # CSV 解析与非空文本校验（中英文表头）
  exceptions.py  # 业务异常与 HTTP 状态码映射
  service.py     # 批次提交 OCC 流程（prepare→commit）、追溯、只读视图
  schemas.py     # Pydantic 响应模型
  executors.py   # 上传/提交/查询三组隔离的固定大小线程池 + 上传准入上限
  routes.py      # API 路由（各阶段分派到对应线程池）
  main.py        # 应用入口、异常处理、/docs 与 /demo 演示页
tests/           # pytest 套件 + 真实 HTTP 并发脚本
examples/        # 演示用 CSV
Dockerfile, docker-compose.yml
```

## 8. 配置项

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `LINEAGE_DATA_DIR` | `./data`（容器内 `/data`） | 数据库目录 |
| `LINEAGE_DB_PATH` | `DATA_DIR/lineage.db` | SQLite 文件全路径 |
| `LINEAGE_BUSY_TIMEOUT_MS` | `15000` | 写锁等待超时（保持原值，不靠缩短它腾资源） |
| `LINEAGE_MAX_RETRIES` | `50` | OCC 冲突后库外重校验重试上限 |
| `LINEAGE_MAX_UPLOAD_BYTES` | `20971520` | 单文件大小上限（20 MiB） |
| `LINEAGE_MAX_UPLOAD_ROWS` | `200000` | 单批有效行数上限 |
| `LINEAGE_UPLOAD_WORKERS` | `4` | 上传池线程数（解析/快照/库外判环） |
| `LINEAGE_COMMIT_WORKERS` | `1` | 提交池线程数（短写事务；SQLite 仅一个写者） |
| `LINEAGE_QUERY_WORKERS` | `4` | 查询池线程数（与上传/提交隔离） |
| `LINEAGE_MAX_INFLIGHT_UPLOADS` | `8` | 已准入上传上限，超出者异步排队、不拒收 |
