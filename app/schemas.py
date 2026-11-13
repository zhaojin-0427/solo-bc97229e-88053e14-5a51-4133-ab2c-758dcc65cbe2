"""API 响应模型。"""

from __future__ import annotations

from pydantic import BaseModel, Field


class BatchReceipt(BaseModel):
    batch_id: int = Field(..., description="批次号")
    received_rows: int = Field(..., description="CSV 中的有效数据行数（不含空行）")
    inserted_edges: int = Field(..., description="本批实际新增的关系数")
    duplicate_edges: int = Field(..., description="与库内或批内重复而被合一的行数")
    graph_version: int = Field(..., description="提交成功后的谱系版本号")
    retry_count: int = Field(..., description="因并发冲突在库外重新校验的次数")


class AffectedResponse(BaseModel):
    sample_code: str
    found: bool = Field(..., description="样本号是否已入库")
    graph_version: int = Field(..., description="本次查询读取到的谱系版本（单一完整提交状态）")
    affected_samples: list[str] = Field(
        ..., description="全部直接及间接受影响的衍生样本号，每个仅出现一次"
    )
    affected_count: int | None = None


class BatchRecord(BaseModel):
    id: int
    received_rows: int
    inserted_edges: int
    duplicate_edges: int
    status: str
    graph_version: int
    created_at: str


class BatchListResponse(BaseModel):
    graph_version: int
    batches: list[BatchRecord]


class EdgeView(BaseModel):
    parent_sample: str
    child_sample: str


class GraphOverview(BaseModel):
    graph_version: int
    sample_count: int
    edge_count: int
    edges: list[EdgeView]


class ErrorResponse(BaseModel):
    error: str
    detail: str
    context: dict | None = None


class HealthResponse(BaseModel):
    status: str
    graph_version: int
    sample_count: int
    edge_count: int
