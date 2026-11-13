"""HTTP 路由：上传批次、追溯查询、批次与全图只读视图、健康检查。"""

from __future__ import annotations

from fastapi import APIRouter, File, Request, UploadFile

from . import service
from .csv_utils import parse_edges_csv
from .schemas import (
    AffectedResponse,
    BatchListResponse,
    BatchReceipt,
    GraphOverview,
    HealthResponse,
)

router = APIRouter(prefix="/api/v1")


@router.post("/batches", response_model=BatchReceipt,
             summary="上传一批样本衍生关系（CSV）",
             responses={409: {"description": "合并后成环，整批拒绝，返回环路"},
                        422: {"description": "CSV 字段校验失败"}})
async def upload_batch(request: Request,
                       file: UploadFile = File(..., description=(
                           "UTF-8 CSV，两列：亲本样本号,衍生样本号"))):
    raw = await file.read()
    # 纯 CPU/内存工作，在线程池执行以不阻塞事件循环
    edges, received_rows = await request.app.state.loop.run_in_executor(
        None, parse_edges_csv, raw
    )
    with request.app.state.session() as conn:
        receipt = await request.app.state.loop.run_in_executor(
            None, service.submit_batch, conn, edges, received_rows
        )
    return receipt


@router.get("/samples/{sample_code}/affected",
            response_model=AffectedResponse,
            summary="追溯样本的全部直接及间接受影响衍生样本")
async def get_affected(sample_code: str, request: Request):
    with request.app.state.session() as conn:
        result = await request.app.state.loop.run_in_executor(
            None, service.find_affected, conn, sample_code
        )
    return result


@router.get("/batches", response_model=BatchListResponse,
            summary="查看已提交批次")
async def get_batches(request: Request, limit: int = 100):
    with request.app.state.session() as conn:
        return await request.app.state.loop.run_in_executor(
            None, service.list_batches, conn, limit
        )


@router.get("/graph", response_model=GraphOverview,
            summary="查看当前完整谱系（版本、节点、边）")
async def get_graph(request: Request):
    with request.app.state.session() as conn:
        return await request.app.state.loop.run_in_executor(
            None, service.get_graph_overview, conn
        )


@router.get("/health", response_model=HealthResponse,
            summary="健康检查")
async def health(request: Request):
    with request.app.state.session() as conn:
        overview = await request.app.state.loop.run_in_executor(
            None, service.get_graph_overview, conn
        )
    return {
        "status": "ok",
        "graph_version": overview["graph_version"],
        "sample_count": overview["sample_count"],
        "edge_count": overview["edge_count"],
    }
