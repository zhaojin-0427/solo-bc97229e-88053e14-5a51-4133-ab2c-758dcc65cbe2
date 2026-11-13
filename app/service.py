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


def submit_batch(conn: sqlite3.Connection,
                 new_edges: list[tuple[str, str]],
                 received_rows: int) -> dict:
    """提交一批关系，返回批次回执；成环则抛 CycleRejectedError。

    校验全部在写事务之外完成；写事务只负责版本检查与插入，
    任一失败都 ROLLBACK，数据库不留半批数据。
    """
    attempts = 0
    while True:
        attempts += 1
        snapshot = _load_snapshot(conn)

        # ---- 库外校验（无库锁）：与已入库完整谱系合并后判环 ----
        base_graph = graph_mod.build_graph(snapshot.edges)
        candidate_graph = graph_mod.merge_edges(base_graph, new_edges)
        cycle = graph_mod.find_cycle(candidate_graph)
        if cycle is not None:
            # 自环同样在此被捕获（find_cycle 返回 [X, X]）
            raise CycleRejectedError(cycle)

        # ---- 短写事务：乐观版本检查后提交 ----
        try:
            conn.execute("BEGIN IMMEDIATE")
            current_version = get_graph_version(conn)
            if current_version != snapshot.version:
                # 期间有别批提交：放弃本事务，用最新完整谱系重判
                conn.execute("ROLLBACK")
                if attempts > config.MAX_COMMIT_RETRIES:
                    raise LineageError(
                        "并发冲突重试次数过多，请稍后重新提交",
                        {"retries": attempts - 1},
                    )
                continue

            sample_id_cache: dict[str, int] = {}
            cur = conn.execute(
                "INSERT INTO batches(received_rows, inserted_edges, "
                "duplicate_edges, status, graph_version) "
                "VALUES (?, 0, 0, 'committed', ?)",
                (received_rows, current_version + 1),
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
            for parent, child in new_edges:
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
                (inserted, received_rows - inserted, batch_id),
            )
            conn.execute(
                "UPDATE meta SET value = ? WHERE key = 'graph_version'",
                (str(current_version + 1),),
            )
            conn.execute("COMMIT")
            return {
                "batch_id": batch_id,
                "received_rows": received_rows,
                "inserted_edges": inserted,
                "duplicate_edges": received_rows - inserted,
                "graph_version": current_version + 1,
                "retry_count": attempts - 1,
            }
        except sqlite3.OperationalError as exc:
            # 写锁竞争（busy_timeout 内未拿到锁）等：安全重试
            _safe_rollback(conn)
            if attempts > config.MAX_COMMIT_RETRIES:
                raise LineageError(f"数据库忙，重试失败：{exc}") from exc
            continue
        except Exception:
            _safe_rollback(conn)
            raise


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
