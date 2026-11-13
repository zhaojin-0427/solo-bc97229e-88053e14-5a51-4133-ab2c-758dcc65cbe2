"""上传 CSV 的解析与字段校验。

要求两列：亲本样本号 / 衍生样本号（兼容英文表头 parent_sample /
child_sample 及常见别名）。每个字段必须为非空文本（去除首尾空白后
仍有内容）。空行跳过；任何不合规行整批拒绝（不会有部分入库）。
"""

from __future__ import annotations

import csv
import io

from . import config
from .exceptions import CsvValidationError, UploadTooLargeError

PARENT_HEADERS = {
    "亲本样本号", "亲本样本", "父样本号", "父样本",
    "parent_sample", "parent", "parent_id", "parent_code",
    "parent sample", "parent sample code",
}
CHILD_HEADERS = {
    "衍生样本号", "衍生样本", "子样本号", "子样本",
    "child_sample", "child", "child_id", "child_code",
    "derived_sample", "derived",
    "child sample", "child sample code", "derived sample",
}


def _norm_header(value: str | None) -> str:
    return (value or "").strip().lower().replace("﻿", "")


def parse_edges_csv(raw: bytes) -> tuple[list[tuple[str, str]], int]:
    """把上传字节解析为 (去重后的关系对列表, 原始数据行数)。

    关系对保序；原始数据行数不含空行，用于统计批内重复行数。
    """
    if len(raw) > config.MAX_UPLOAD_BYTES:
        raise UploadTooLargeError(
            f"上传文件超过 {config.MAX_UPLOAD_BYTES} 字节上限"
        )

    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError(
            "CSV 必须为 UTF-8 编码文本", {"reason": str(exc)}
        ) from exc

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        raise CsvValidationError("CSV 内容为空，缺少表头") from None

    norm = [_norm_header(h) for h in header]
    try:
        parent_idx = next(i for i, h in enumerate(norm) if h in PARENT_HEADERS)
        child_idx = next(i for i, h in enumerate(norm) if h in CHILD_HEADERS)
    except StopIteration:
        raise CsvValidationError(
            "表头必须包含亲本样本号与衍生样本号两列",
            {"found_header": header},
        ) from None

    edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    total_rows = 0
    for line_no, row in enumerate(reader, start=2):  # 表头是第 1 行
        if not row or all(not (cell or "").strip() for cell in row):
            continue  # 空行跳过
        total_rows += 1
        if len(row) <= max(parent_idx, child_idx):
            raise CsvValidationError(
                f"第 {line_no} 行列数不足，需要亲本样本号与衍生样本号两列",
                {"line": line_no, "row": row},
            )
        parent = (row[parent_idx] or "").strip()
        child = (row[child_idx] or "").strip()
        if not parent or not child:
            raise CsvValidationError(
                f"第 {line_no} 行存在空样本号，亲本与衍生样本号均须为非空文本",
                {"line": line_no, "row": row},
            )
        pair = (parent, child)
        if pair not in seen:
            seen.add(pair)
            edges.append(pair)
        if len(edges) > config.MAX_UPLOAD_ROWS:
            raise UploadTooLargeError(
                f"单批有效关系数超过 {config.MAX_UPLOAD_ROWS} 行上限"
            )

    if not edges:
        raise CsvValidationError("CSV 没有任何有效数据行")
    return edges, total_rows
