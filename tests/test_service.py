"""服务层测试：提交、去重、环拒绝、追溯、并发与原子性。"""

from __future__ import annotations

import threading

import pytest

from app import service
from app.exceptions import CycleRejectedError


def _submit(database, db_path, edges, received_rows=None):
    conn = database.connect(str(db_path))
    try:
        return service.submit_batch(
            conn, edges, received_rows if received_rows is not None else len(edges)
        )
    finally:
        conn.close()


def test_basic_commit_and_version(temp_db):
    database, db_path = temp_db
    r = _submit(database, db_path, [("A", "B"), ("A", "C")])
    assert r["inserted_edges"] == 2
    assert r["graph_version"] == 1
    r2 = _submit(database, db_path, [("B", "D")])
    assert r2["graph_version"] == 2


def test_duplicate_edges_collapse(temp_db):
    database, db_path = temp_db
    _submit(database, db_path, [("A", "B"), ("A", "B")], received_rows=2)
    conn = database.connect(str(db_path))
    try:
        assert conn.execute("SELECT COUNT(*) n FROM edges").fetchone()["n"] == 1
    finally:
        conn.close()
    # 跨批次重复同样合一
    r = _submit(database, db_path, [("A", "B"), ("B", "C")], received_rows=2)
    assert r["inserted_edges"] == 1
    assert r["duplicate_edges"] == 1


def test_multi_parent_allowed(temp_db):
    database, db_path = temp_db
    _submit(database, db_path, [("P1", "C"), ("P2", "C"), ("P3", "C")])
    conn = database.connect(str(db_path))
    try:
        parents = conn.execute(
            "SELECT p.code FROM edges e JOIN samples p ON p.id=e.parent_id "
            "JOIN samples c ON c.id=e.child_id WHERE c.code='C' ORDER BY p.code"
        ).fetchall()
        assert [r["code"] for r in parents] == ["P1", "P2", "P3"]
    finally:
        conn.close()


def test_self_loop_rejects_whole_batch(temp_db):
    database, db_path = temp_db
    _submit(database, db_path, [("A", "B")])
    with pytest.raises(CycleRejectedError) as ei:
        _submit(database, db_path, [("B", "C"), ("C", "C")])
    assert ei.value.cycle == ["C", "C"]
    # 整批拒绝：B->C 也不得入库
    conn = database.connect(str(db_path))
    try:
        assert conn.execute("SELECT COUNT(*) n FROM edges").fetchone()["n"] == 1
        assert conn.execute("SELECT COUNT(*) n FROM batches").fetchone()["n"] == 1
        assert service.get_graph_version(conn) == 1
    finally:
        conn.close()


def test_cycle_batch_returns_real_cycle_and_nothing_persisted(temp_db):
    database, db_path = temp_db
    _submit(database, db_path, [("A", "B"), ("B", "C")])
    with pytest.raises(CycleRejectedError) as ei:
        # C -> A 与已有 A->B->C 成环
        _submit(database, db_path, [("C", "A"), ("X", "Y")])
    cycle = ei.value.cycle
    assert cycle[0] == cycle[-1]
    assert set(cycle[:-1]) <= {"A", "B", "C"}
    conn = database.connect(str(db_path))
    try:
        # 同批的合法边 X->Y 也必须随整批回滚
        assert conn.execute("SELECT COUNT(*) n FROM edges").fetchone()["n"] == 2
        assert conn.execute(
            "SELECT COUNT(*) n FROM samples WHERE code IN ('X','Y')"
        ).fetchone()["n"] == 0
    finally:
        conn.close()


def test_affected_transitive_dedup(temp_db):
    database, db_path = temp_db
    # 菱形：A->B,A->D,B->C,D->C，另加 A->E
    _submit(database, db_path,
            [("A", "B"), ("A", "D"), ("B", "C"), ("D", "C"), ("A", "E")])
    conn = database.connect(str(db_path))
    try:
        result = service.find_affected(conn, "A")
        assert result["found"] is True
        assert len(result["affected_samples"]) == 4
        assert len(set(result["affected_samples"])) == 4  # 每个仅一次
        assert set(result["affected_samples"]) == {"B", "C", "D", "E"}
        # 不包含自身
        assert "A" not in result["affected_samples"]
        # 中间节点只返回其下游
        mid = service.find_affected(conn, "B")
        assert mid["affected_samples"] == ["C"]
        # 未知样本
        unknown = service.find_affected(conn, "NOPE")
        assert unknown["found"] is False and unknown["affected_samples"] == []
    finally:
        conn.close()


def test_concurrent_batches_only_one_succeeds_when_merged_cycles(temp_db):
    """两批分别合法、合并成环：并发提交时必须恰好一批成功、一批被拒。"""
    database, db_path = temp_db
    _submit(database, db_path, [("A", "B")])

    barrier = threading.Barrier(2)
    outcomes: list[str] = []
    lock = threading.Lock()

    # 批次 1：B -> C（合法）；批次 2：C -> A（单独看合法，合并后成环）
    def worker(edges, tag):
        conn = database.connect(str(db_path))
        try:
            barrier.wait()
            try:
                service.submit_batch(conn, edges, len(edges))
                with lock:
                    outcomes.append(f"{tag}:ok")
            except CycleRejectedError as exc:
                with lock:
                    outcomes.append(f"{tag}:rejected:{'-'.join(exc.cycle)}")
        finally:
            conn.close()

    t1 = threading.Thread(target=worker, args=([("B", "C")], "batch1"))
    t2 = threading.Thread(target=worker, args=([("C", "A")], "batch2"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(outcomes) == 2
    assert sum(1 for o in outcomes if o.endswith("ok")) == 1
    assert any("rejected" in o for o in outcomes)

    conn = database.connect(str(db_path))
    try:
        # 最终图仍然无环且只含成功批次的数据
        from app.graph import build_graph, find_cycle
        rows = conn.execute(
            "SELECT p.code, c.code FROM edges e "
            "JOIN samples p ON p.id=e.parent_id "
            "JOIN samples c ON c.id=e.child_id"
        ).fetchall()
        g = build_graph([(r[0], r[1]) for r in rows])
        assert find_cycle(g) is None
        assert conn.execute("SELECT COUNT(*) n FROM edges").fetchone()["n"] == 2
        assert database.get_graph_version(conn) == 2
    finally:
        conn.close()


def test_concurrent_independent_batches_both_succeed(temp_db):
    """互不相关的两批并发提交都应成功（合法批次不能仅因别批被拒）。"""
    database, db_path = temp_db
    barrier = threading.Barrier(2)
    errors: list[Exception] = []

    def worker(edges):
        conn = database.connect(str(db_path))
        try:
            barrier.wait()
            service.submit_batch(conn, edges, len(edges))
        except Exception as exc:
            errors.append(exc)
        finally:
            conn.close()

    t1 = threading.Thread(target=worker, args=([("A", "B")],))
    t2 = threading.Thread(target=worker, args=([("X", "Y")],))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert errors == []
    conn = database.connect(str(db_path))
    try:
        assert conn.execute("SELECT COUNT(*) n FROM edges").fetchone()["n"] == 2
        assert database.get_graph_version(conn) == 2
    finally:
        conn.close()


def test_duplicate_detected_after_occ_retry(temp_db, monkeypatch):
    """OCC 重试后，别批已插入的同一条边必须被计为重复而非触发约束错误。"""
    database, db_path = temp_db

    # 先由“别批”提交 A->B
    _submit(database, db_path, [("A", "B")])

    real_load = service._load_snapshot
    state = {"first": True}

    def patched_load(conn):
        snap = real_load(conn)
        if state["first"]:
            state["first"] = False
            # 返回过期快照（模拟本批首次校验后、写事务前别批刚提交的情形）：
            # 版本号回退一格，边集不含 A->B，强制写事务版本冲突并重试
            return service.Snapshot(
                version=snap.version - 1, edges=[]
            )
        return snap

    monkeypatch.setattr(service, "_load_snapshot", patched_load)
    conn = database.connect(str(db_path))
    try:
        receipt = service.submit_batch(conn, [("A", "B"), ("B", "C")], 2)
    finally:
        conn.close()

    assert receipt["retry_count"] >= 1
    assert receipt["inserted_edges"] == 1       # 仅 B->C 新增
    assert receipt["duplicate_edges"] == 1      # A->B 与别批重复，合一
    conn = database.connect(str(db_path))
    try:
        assert conn.execute("SELECT COUNT(*) n FROM edges").fetchone()["n"] == 2
        assert database.get_graph_version(conn) == 2
    finally:
        conn.close()


def test_rollback_leaves_no_half_batch_on_failure(temp_db):
    """模拟插入中途异常：事务回滚后不留半批、版本不变。"""
    database, db_path = temp_db
    _submit(database, db_path, [("A", "B")])
    conn = database.connect(str(db_path))
    try:
        snapshot = service._load_snapshot(conn)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO batches(received_rows, inserted_edges, duplicate_edges, "
            "status, graph_version) VALUES (2,0,0,'committed', ?)",
            (snapshot.version + 1,),
        )
        conn.execute("INSERT OR IGNORE INTO samples(code) VALUES ('B')")
        # 模拟进程/语句故障
        conn.execute("ROLLBACK")

        assert conn.execute("SELECT COUNT(*) n FROM batches").fetchone()["n"] == 1
        assert database.get_graph_version(conn) == 1
    finally:
        conn.close()


def test_reopen_database_preserves_committed_data(temp_db):
    """重启等价场景：重新打开连接后已提交关系保留。"""
    database, db_path = temp_db
    _submit(database, db_path, [("A", "B"), ("B", "C")])
    fresh = database.connect(str(db_path))
    try:
        result = service.find_affected(fresh, "A")
        assert set(result["affected_samples"]) == {"B", "C"}
        assert database.get_graph_version(fresh) == 1
    finally:
        fresh.close()


def test_affected_deep_chain_via_recursive_cte(temp_db):
    """超长链追溯：递归 CTE 返回全部下游、去重且不含根。"""
    database, db_path = temp_db
    depth = 3000
    edges = [(f"N{i}", f"N{i+1}") for i in range(depth)]
    _submit(database, db_path, edges)
    conn = database.connect(str(db_path))
    try:
        result = service.find_affected(conn, "N0")
        assert result["found"] is True
        assert result["affected_count"] == depth
        assert len(set(result["affected_samples"])) == depth
        assert "N0" not in result["affected_samples"]
        assert result["affected_samples"][0] == "N1"
        assert result["affected_samples"][-1] == f"N{depth}"
    finally:
        conn.close()
