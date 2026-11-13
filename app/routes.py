"""HTTP 路由：上传批次、追溯查询、批次与全图只读视图、健康检查。

线程模型（关键）：上传与查询走彼此独立的固定大小线程池，并且每个池内
的调用自行建立、使用、关闭自己的 SQLite 连接（连接不跨线程交接）：

- 校验阶段（解析 CSV + 快照读 + 库外判环）在上传池执行，从不持有写锁；
- 提交阶段（BEGIN IMMEDIATE 短写事务）在提交池执行，写锁等待只发生在这里；
- 追溯 / 批次 / 全图 / 健康检查只在查询池执行。

因此即使提交线程全部卡在写锁等待上，查询池仍可并发读取完整的已提交
状态；上传准入另有显式上限，超出者在事件循环中异步排队而非被拒收。
"""

from __future__ import annotations

from fastapi import APIRouter, File, Request, UploadFile

from . import config, database, service
from .csv_utils import parse_edges_csv
from .exceptions import LineageError
from .schemas import (
    AffectedResponse,
    BatchListResponse,
    BatchReceipt,
    GraphOverview,
    HealthResponse,
)

router = APIRouter(prefix="/api/v1")


# ---------------------------------------------------------------- 池内工作单元

def _parse_upload(raw: bytes):
    """上传池：CSV 解析与字段校验（纯内存，不触库、不持锁）。"""
    return parse_edges_csv(raw)


def _prepare(db_path: str, edges, received_rows: int):
    """上传池：快照读 + 库外判环（不进入写事务，不持写锁）。"""
    conn = database.connect(db_path)
    try:
        return service.prepare_batch(conn, edges, received_rows)
    finally:
        conn.close()


def _commit(db_path: str, prepared: service.PreparedBatch):
    """提交池：仅短写事务；写锁等待被限制在这个池的线程上。"""
    conn = database.connect(db_path)
    try:
        return service.commit_prepared(conn, prepared)
    finally:
        conn.close()


def _query(db_path: str, fn, *args):
    """查询池：只读工作使用独立连接，与上传/提交互不共享线程。"""
    conn = database.connect(db_path)
    try:
        return fn(conn, *args)
    finally:
        conn.close()


def _pool(request: Request):
    return request.app.state.executors, request.app.state.db_path


# ---------------------------------------------------------------- 路由

@router.post("/batches", response_model=BatchReceipt,
             summary="上传一批样本衍生关系（CSV）",
             responses={409: {"description": "合并后成环，整批拒绝，返回环路"},
                        422: {"description": "CSV 字段校验失败"}})
async def upload_batch(request: Request,
                       file: UploadFile = File(..., description=(
                           "UTF-8 CSV，两列：亲本样本号,衍生样本号"))):
    raw = await file.read()
    pool, db_path = _pool(request)

    # 准入有上限：超出者在事件循环中等待（不占任何工作线程），不拒收
    async with pool.upload_slot():
        # CSV 解析只需一次；OCC 重试只重做“快照 + 判环 + 短写事务”
        edges, received_rows = await pool.run_upload(_parse_upload, raw)
        retries = 0
        while True:
            # ① 快照 + 库外判环：慢校验不阻塞别批提交，也不占写锁
            prepared = await pool.run_upload(
                _prepare, db_path, edges, received_rows
            )
            # ② 短写事务：写锁只在此阶段线程上等待
            try:
                receipt = await pool.run_commit(_commit, db_path, prepared)
            except service.CommitConflict as exc:
                retries += 1
                if retries > config.MAX_COMMIT_RETRIES:
                    raise LineageError(
                        "并发冲突重试次数过多，请稍后重新提交",
                        {"retries": retries - 1},
                    ) from exc
                # 版本已变/写锁暂不可得：用最新完整谱系重新校验后再提交
                continue
            receipt["retry_count"] = retries
            return receipt


@router.get("/samples/{sample_code}/affected",
            response_model=AffectedResponse,
            summary="追溯样本的全部直接及间接受影响衍生样本")
async def get_affected(sample_code: str, request: Request):
    pool, db_path = _pool(request)
    return await pool.run_query(
        _query, db_path, service.find_affected, sample_code
    )


@router.get("/batches", response_model=BatchListResponse,
            summary="查看已提交批次")
async def get_batches(request: Request, limit: int = 100):
    pool, db_path = _pool(request)
    return await pool.run_query(_query, db_path, service.list_batches, limit)


@router.get("/graph", response_model=GraphOverview,
            summary="查看当前完整谱系（版本、节点、边）")
async def get_graph(request: Request):
    pool, db_path = _pool(request)
    return await pool.run_query(
        _query, db_path, service.get_graph_overview
    )


@router.get("/health", response_model=HealthResponse,
            summary="健康检查")
async def health(request: Request):
    pool, db_path = _pool(request)
    snapshot = await pool.run_query(_query, db_path, service.health_snapshot)
    return {"status": "ok", **snapshot}
