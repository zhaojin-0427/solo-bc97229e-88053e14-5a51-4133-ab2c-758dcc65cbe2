"""谱系核心服务：批次提交（乐观并发 + 库外环校验）与追溯查询。

并发模型（需求：图校验不得占库写事务；合法批次不能仅因别批提交被拒）：

1. 快照读：在一个延迟读事务中读取“已提交完整谱系”的边集与图版本，
   随即释放读事务。WAL 模式下该读不阻塞任何写。
2. 库外校验：在 Python 内存中把本批边与快照合并并做环检测，
   全程不持有任何数据库锁，其他批次可正常提交。
3. 提交：BEGIN IMMEDIATE 立即取写锁（仅短事务写库），先比对图版本：
   - 版本未变 => 插入并把版本 +1，提交；
   - 版本已变（期间有别批提交）=> 回滚释放锁，回到第 1 步用最新
     完整谱系重新校验。若新谱系下仍合法则本批照样成功；若成环则
     返回真实环路。因此“分别合法、合并成环”的两批只有先提交者成功，
     而合法批次不会仅因并发冲突被直接拒绝。

第 1、2 步在 HTTP 路径上由上传池执行，第 3 步由提交池执行（见
executors.py / routes.py），因此写锁等待只会占用提交池线程。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from . import config
from . import graph as graph_mod
from .database import get_graph_version
from .exceptions import CycleRejectedError, LineageError


@dataclass
class Snapshot:
    version: int
    edges: list[tuple[str, str]]


@dataclass
class PreparedBatch:
    """已完成库外校验、等待短写事务提交的一批关系。"""

    version: int                    # 校验所基于的已提交谱系版本
    edges: list[tuple[str, str]]
    received_rows: int


class CommitConflict(Exception):
    """提交阶段发现版本已变或写锁暂不可得：本批尚未写入，应重试。"""


# ---------------------------------------------------------------- 快照读取


def _load_snapshot(conn: sqlite3.Connection) -> Snapshot:
    """读取一个完整提交状态的谱系快照（短读事务，WAL 下不阻塞写）。"""
    conn.execute("BEGIN DEFERRED")
    try:
        version = get_graph_version(conn)
        rows = conn.execute(
            """
            SELECT p.code AS parent_code, c.code AS child_code
            FROM edges e
            JOIN samples p ON p.id = e.parent_id
            JOIN samples c ON c.id = e.child_id
            """
        ).fetchall()
        edges = [(r["parent_code"], r["child_code"]) for r in rows]
    finally:
        conn.execute("COMMIT")
    return Snapshot(version=version, edges=edges)


# ---------------------------------------------------------------- 批次提交

def _get_or_create_sample_id(conn: sqlite3.Connection, code: str,
                             cache: dict[str, int]) -> int:
    if code in cache:
        return cache[code]
    conn.execute("INSERT OR IGNORE INTO samples(code) VALUES (?)", (code,))
    row = conn.execute(
        "SELECT id FROM samples WHERE code = ?", (code,)
    ).fetchone()
    cache[code] = row["id"]
    return row["id"]


def prepare_batch(conn: sqlite3.Connection,
                  new_edges: list[tuple[str, str]],
                  received_rows: int) -> PreparedBatch:
    """读取完整已提交谱系，并在内存中把本批边合并后判环。

    全程只使用一个延迟读事务（读完即提交释放），不进入写事务、不持写锁，
    因此单批的校验耗时不会阻塞其他批次的提交或任何查询。成环整批拒绝。
    """
    snapshot = _load_snapshot(conn)
    base_graph = graph_mod.build_graph(snapshot.edges)
    candidate_graph = graph_mod.merge_edges(base_graph, new_edges)
    cycle = graph_mod.find_cycle(candidate_graph)
    if cycle is not None:
        # 自环同样在此被捕获（find_cycle 返回 [X, X]）
        raise CycleRejectedError(cycle)
    return PreparedBatch(
        version=snapshot.version,
        edges=list(new_edges),
        received_rows=received_rows,
    )


def commit_prepared(conn: sqlite3.Connection, prepared: PreparedBatch) -> dict:
    """用一个短写事务提交已校验的批次（写锁只在此处被持有）。

    事务内先比对图版本：与校验时不一致说明期间有别批提交，回滚并抛
    CommitConflict，由调用方基于最新完整谱系重新校验后重试；因此
    合法批次不会仅因并发冲突被拒绝，而“分别合法、合并成环”的批次
    会在重试时以真实环路被拒。任何失败都 ROLLBACK，不留半批数据。
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
        current_version = get_graph_version(conn)
        if current_version != prepared.version:
            raise CommitConflict("snapshot version changed")

        sample_id_cache: dict[str, int] = {}
        cur = conn.execute(
            "INSERT INTO batches(received_rows, inserted_edges, "
            "duplicate_edges, status, graph_version) "
            "VALUES (?, 0, 0, 'committed', ?)",
            (prepared.received_rows, current_version + 1),
        )
        batch_id = cur.lastrowid

        # 以当前写事务内可见的最新已提交边为基准判重，
        # 避免 OCC 重试场景下沿用过期快照导致 UNIQUE 冲突
        existing: set[tuple[str, str]] = {
            (r["parent_code"], r["child_code"])
            for r in conn.execute(
                """
                SELECT p.code AS parent_code, c.code AS child_code
                FROM edges e
                JOIN samples p ON p.id = e.parent_id
                JOIN samples c ON c.id = e.child_id
                """
            ).fetchall()
        }
        inserted = 0
        for parent, child in prepared.edges:
            if (parent, child) in existing:
                continue  # 重复关系合一：库内已有（含批内重复），跳过
            existing.add((parent, child))
            pid = _get_or_create_sample_id(conn, parent, sample_id_cache)
            cid = _get_or_create_sample_id(conn, child, sample_id_cache)
            conn.execute(
                "INSERT INTO edges(parent_id, child_id, batch_id) "
                "VALUES (?, ?, ?)",
                (pid, cid, batch_id),
            )
            inserted += 1

        conn.execute(
            "UPDATE batches SET inserted_edges = ?, duplicate_edges = ? "
            "WHERE id = ?",
            (inserted, prepared.received_rows - inserted, batch_id),
        )
        conn.execute(
            "UPDATE meta SET value = ? WHERE key = 'graph_version'",
            (str(current_version + 1),),
        )
        conn.execute("COMMIT")
        return {
            "batch_id": batch_id,
            "received_rows": prepared.received_rows,
            "inserted_edges": inserted,
            "duplicate_edges": prepared.received_rows - inserted,
            "graph_version": current_version + 1,
            "retry_count": 0,
        }
    except CommitConflict:
        _safe_rollback(conn)
        raise
    except sqlite3.OperationalError as exc:
        # 写锁竞争（busy_timeout 内未拿到锁）等：本事务未写入，可安全重试
        _safe_rollback(conn)
        raise CommitConflict(str(exc)) from exc
    except BaseException:
        _safe_rollback(conn)
        raise


def submit_batch(conn: sqlite3.Connection,
                 new_edges: list[tuple[str, str]],
                 received_rows: int,
                 *,
                 max_prepare_retries: int | None = None) -> dict:
    """提交一批关系，返回批次回执；成环则抛 CycleRejectedError。

    进程内调用路径（单连接；prepare 与 commit 在同一线程依次执行）。
    HTTP 路径把这两个阶段分别交给上传池与提交池执行，见 routes.py。
    """
    if max_prepare_retries is None:
        max_prepare_retries = config.MAX_COMMIT_RETRIES
    prepare_retries = 0
    while True:
        prepared = prepare_batch(conn, new_edges, received_rows)
        try:
            receipt = commit_prepared(conn, prepared)
        except CommitConflict as exc:
            prepare_retries += 1
            if prepare_retries > max_prepare_retries:
                raise LineageError(
                    "并发冲突重试次数过多，请稍后重新提交",
                    {"retries": prepare_retries - 1},
                ) from exc
            continue
        receipt["retry_count"] = prepare_retries
        return receipt


def _safe_rollback(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


# ---------------------------------------------------------------- 追溯查询

def find_affected(conn: sqlite3.Connection, sample_code: str) -> dict:
    """返回某样本的全部直接及间接受影响衍生样本（去重，不含自身）。

    使用递归 CTE 只遍历可达子图，并在单个只读事务内完成，因此一次查询
    只会看到某一个完整提交状态，绝不会读到半批数据。入库不变式保证图为
    DAG，递归项的 UNION 去重使每个节点最多展开一次即自然终止，无需在
    递归中维护路径字符串，复杂度对可达子图近似线性。
    """
    conn.execute("BEGIN DEFERRED")
    try:
        version = get_graph_version(conn)
        root = conn.execute(
            "SELECT id FROM samples WHERE code = ?", (sample_code,)
        ).fetchone()
        if root is None:
            conn.execute("COMMIT")
            return {
                "sample_code": sample_code,
                "found": False,
                "graph_version": version,
                "affected_samples": [],
            }

        # 锚点取根的直接子样本；递归沿 parent->child 展开。
        # UNION（非 UNION ALL）按 child_id 去重，每个衍生样本只保留一次，
        # 且根不在锚点中，结果天然不含自身。
        rows = conn.execute(
            """
            WITH RECURSIVE reachable(child_id, depth) AS (
                SELECT e.child_id, 1
                FROM edges e
                WHERE e.parent_id = ?
                UNION
                SELECT e.child_id, r.depth + 1
                FROM reachable r
                JOIN edges e ON e.parent_id = r.child_id
            )
            SELECT s.code AS code, MIN(r.depth) AS min_depth
            FROM reachable r
            JOIN samples s ON s.id = r.child_id
            GROUP BY r.child_id, s.code
            ORDER BY min_depth, r.child_id
            """,
            (root["id"],),
        ).fetchall()
        ordered = [r["code"] for r in rows]

        conn.execute("COMMIT")
        return {
            "sample_code": sample_code,
            "found": True,
            "graph_version": version,
            "affected_samples": ordered,
            "affected_count": len(ordered),
        }
    except Exception:
        _safe_rollback(conn)
        raise


# ---------------------------------------------------------------- 只读视图

def list_batches(conn: sqlite3.Connection, limit: int = 100) -> dict:
    conn.execute("BEGIN DEFERRED")
    try:
        version = get_graph_version(conn)
        rows = conn.execute(
            "SELECT id, received_rows, inserted_edges, duplicate_edges, "
            "status, graph_version, created_at "
            "FROM batches ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.execute("COMMIT")
        return {
            "graph_version": version,
            "batches": [dict(r) for r in rows],
        }
    except Exception:
        _safe_rollback(conn)
        raise


def get_graph_overview(conn: sqlite3.Connection) -> dict:
    conn.execute("BEGIN DEFERRED")
    try:
        version = get_graph_version(conn)
        sample_count = conn.execute(
            "SELECT COUNT(*) AS n FROM samples"
        ).fetchone()["n"]
        edge_count = conn.execute(
            "SELECT COUNT(*) AS n FROM edges"
        ).fetchone()["n"]
        rows = conn.execute(
            """
            SELECT p.code AS parent_code, c.code AS child_code
            FROM edges e
            JOIN samples p ON p.id = e.parent_id
            JOIN samples c ON c.id = e.child_id
            ORDER BY e.id
            """
        ).fetchall()
        conn.execute("COMMIT")
        return {
            "graph_version": version,
            "sample_count": sample_count,
            "edge_count": edge_count,
            "edges": [
                {"parent_sample": r["parent_code"],
                 "child_sample": r["child_code"]}
                for r in rows
            ],
        }
    except Exception:
        _safe_rollback(conn)
        raise


def health_snapshot(conn: sqlite3.Connection) -> dict:
    """健康检查所需的最小读取：单一读事务内的完整提交状态。

    不拉取全部边（避免健康检查在大图上变慢），但版本、样本数与边数
    取自同一快照，彼此一致。
    """
    conn.execute("BEGIN DEFERRED")
    try:
        version = get_graph_version(conn)
        sample_count = conn.execute(
            "SELECT COUNT(*) AS n FROM samples"
        ).fetchone()["n"]
        edge_count = conn.execute(
            "SELECT COUNT(*) AS n FROM edges"
        ).fetchone()["n"]
        conn.execute("COMMIT")
        return {
            "graph_version": version,
            "sample_count": sample_count,
            "edge_count": edge_count,
        }
    except Exception:
        _safe_rollback(conn)
        raise
