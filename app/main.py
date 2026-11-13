"""FastAPI 应用入口。

启动：uvicorn app.main:app --host 0.0.0.0 --port 8000
文档：/docs（Swagger UI）、/redoc、/demo（内置交互演示页）
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

from . import __version__, config, database
from .exceptions import LineageError
from .executors import Executors
from .routes import router

DEMO_PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>样本谱系 API 演示</title>
<style>
  body { font-family: system-ui, "PingFang SC", "Microsoft YaHei", sans-serif;
         margin: 2rem; max-width: 980px; color: #1f2937; }
  h1 { font-size: 1.4rem; }
  textarea { width: 100%; height: 160px; font-family: ui-monospace, monospace;
             font-size: .9rem; }
  button { padding: .5rem 1rem; margin: .5rem .5rem .5rem 0; cursor: pointer;
           border: 1px solid #2563eb; background: #2563eb; color: #fff;
           border-radius: 6px; }
  button.secondary { background: #fff; color: #2563eb; }
  pre { background: #f3f4f6; padding: 1rem; border-radius: 8px;
        overflow: auto; min-height: 3rem; white-space: pre-wrap; }
  .row { display: flex; gap: .5rem; align-items: center; margin: .5rem 0; }
  input { padding: .5rem; border: 1px solid #d1d5db; border-radius: 6px; }
  .ok { color: #047857; } .bad { color: #b91c1c; }
  small { color: #6b7280; }
</style>
</head>
<body>
<h1>样本衍生谱系 API 演示 <small>v</small></h1>

<h2>1. 上传批次（CSV）</h2>
<textarea id="csv">亲本样本号,衍生样本号
S001,S002
S001,S003
S002,S004
S003,S004</textarea>
<div>
  <button onclick="upload()">上传合法批次</button>
  <button class="secondary" onclick="loadCycle()">填入成环批次</button>
  <button class="secondary" onclick="loadSelfLoop()">填入自环批次</button>
  <button class="secondary" onclick="loadDup()">填入含重复关系批次</button>
</div>
<h3>响应</h3><pre id="up">（待操作）</pre>

<h2>2. 追溯受影响样本</h2>
<div class="row">
  <input id="code" value="S001" size="16">
  <button onclick="trace()">查询 /samples/{code}/affected</button>
  <button class="secondary" onclick="overview()">查看全图 /graph</button>
  <button class="secondary" onclick="batches()">查看批次 /batches</button>
</div>
<h3>响应</h3><pre id="q">（待操作）</pre>

<script>
const $ = id => document.getElementById(id);
async function show(el, resp) {
  const text = await resp.text();
  el.className = resp.ok ? 'ok' : 'bad';
  try { el.textContent = JSON.stringify(JSON.parse(text), null, 2); }
  catch { el.textContent = text; }
}
async function upload() {
  const blob = new Blob([$('csv').value], {type: 'text/csv'});
  const fd = new FormData();
  fd.append('file', blob, 'batch.csv');
  const resp = await fetch('/api/v1/batches', {method: 'POST', body: fd});
  show($('up'), resp);
}
async function trace() {
  const code = encodeURIComponent($('code').value.trim());
  show($('q'), await fetch(`/api/v1/samples/${code}/affected`));
}
async function overview() { show($('q'), await fetch('/api/v1/graph')); }
async function batches() { show($('q'), await fetch('/api/v1/batches')); }
function loadCycle() {
  $('csv').value = '亲本样本号,衍生样本号\nS004,S005\nS005,S001';
}
function loadSelfLoop() {
  $('csv').value = '亲本样本号,衍生样本号\nS009,S009';
}
function loadDup() {
  $('csv').value = '亲本样本号,衍生样本号\nS001,S002\nS001,S002\nS002,S006';
}
</script>
</body>
</html>
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 每个池内工作单元新建短连接并在同一线程内关闭，
    # 避免共享连接跨线程交接
    loop = asyncio.get_running_loop()
    app.state.loop = loop
    app.state.db_path = str(config.DB_PATH)
    # 上传 / 提交 / 查询使用彼此隔离的固定大小线程池（资源总量显式有界）
    executors = Executors()
    executors.bind(loop)
    app.state.executors = executors
    with database.session() as conn:
        database.init_db(conn)
    try:
        yield
    finally:
        executors.shutdown()


app = FastAPI(
    title="LIMS 样本衍生谱系 API",
    version=__version__,
    description=(
        "上传样本亲本-衍生关系（只增不改、重复合一），合并后整图无环方可入库；"
        "支持按样本号追溯全部直接及间接受影响的衍生样本。"
    ),
    lifespan=lifespan,
)

app.include_router(router)


@app.exception_handler(LineageError)
async def lineage_error_handler(request: Request, exc: LineageError):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.error_code,
            "detail": exc.message,
            "context": exc.details,
        },
    )


@app.get("/", include_in_schema=False)
async def root():
    return {
        "service": "lims-lineage",
        "version": __version__,
        "docs": "/docs",
        "demo": "/demo",
    }


@app.get("/demo", include_in_schema=False, response_class=HTMLResponse)
async def demo_page():
    return DEMO_PAGE.replace(
        "<small>v</small>", f"<small>v{__version__}</small>"
    )
