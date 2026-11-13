"""执行资源隔离回归测试：上传耗尽执行容量时查询仍可读完整已提交状态。

用真实 ASGI 应用（同一事件循环 + 同一 Executors 实例）配合一个外部
SQLite 连接持有写锁，精确控制以下交错：

1. 写锁未释放、上传仍在等待 => 追溯/批次/全图/健康检查仍须返回完整
   的已提交状态；
2. 同时发起超过准入上限的上传 => 只在事件循环中排队，不占线程、不拒收；
3. 锁释放后 => 原先等待的合法批次照常提交，版本与边数正确。

这些测试在旧的“所有工作共用一个默认线程池”实现下会超时失败。
"""

from __future__ import annotations

import asyncio
import importlib
import itertools
import os
import sqlite3
import threading
import time

import httpx

CSV_HEADER = "亲本样本号,衍生样本号\n"


def _csv(*edges: tuple[str, str]) -> bytes:
    return (CSV_HEADER + "".join(f"{a},{b}\n" for a, b in edges)).encode()


def _load_app(tmp_path, monkeypatch, **env):
    """按给定配置重新加载 config/database/main，返回全新的 FastAPI 应用。"""
    monkeypatch.setenv("LINEAGE_DB_PATH", str(tmp_path / "lineage.db"))
    monkeypatch.setenv("LINEAGE_DATA_DIR", str(tmp_path))
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))
    from app import config, database, main, service  # noqa: F401
    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(main)
    return main.app, str(tmp_path / "lineage.db")


class _AsyncClient:
    """lifespan + ASGITransport + AsyncClient，供单个异步场景使用。"""

    def __init__(self, app):
        self.app = app
        self.client: httpx.AsyncClient | None = None

    async def __aenter__(self):
        self._lifespan = self.app.router.lifespan_context(self.app)
        await self._lifespan.__aenter__()
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url="http://lineage.test",
            timeout=30.0,
        )
        await self.client.__aenter__()
        return self.client

    async def __aexit__(self, *exc_info):
        await self.client.__aexit__(*exc_info)
        await self._lifespan.__aexit__(*exc_info)


async def _upload(client, *edges: tuple[str, str]):
    return await client.post(
        "/api/v1/batches",
        files={"file": ("batch.csv", _csv(*edges), "text/csv")},
    )


async def _wait_for(flag, timeout: float = 5.0) -> None:
    deadline = time.perf_counter() + timeout
    while not flag.is_set():
        if time.perf_counter() > deadline:
            raise AssertionError("等待上传进入提交阶段超时")
        await asyncio.sleep(0.005)


class _WriteLock:
    """外部连接持有的 BEGIN IMMEDIATE 写锁，模拟“尚未释放”的写事务。"""

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, isolation_level=None, timeout=0)
        self.conn.row_factory = sqlite3.Row
        self._active = False

    def __enter__(self):
        self.conn.execute("BEGIN IMMEDIATE")
        # 未提交的写入：任何查询都不得看到它
        self.conn.execute(
            "INSERT OR IGNORE INTO samples(code) VALUES ('__uncommitted__')"
        )
        self._active = True
        return self

    def release(self) -> None:
        if self._active:
            self.conn.execute("ROLLBACK")
            self._active = False

    def __exit__(self, *exc_info):
        try:
            self.release()
        finally:
            self.conn.close()


def test_queries_complete_while_upload_waits_for_write_lock(tmp_path, monkeypatch):
    """持锁时发起查询：必须立即返回完整的已提交状态，不被上传拖住。"""
    app, db_path = _load_app(
        tmp_path, monkeypatch,
        LINEAGE_UPLOAD_WORKERS=2,
        LINEAGE_COMMIT_WORKERS=1,
        LINEAGE_QUERY_WORKERS=2,
        LINEAGE_MAX_INFLIGHT_UPLOADS=4,
    )
    from app import service

    real_commit = service.commit_prepared
    commit_entered = threading.Event()

    def observed_commit(conn, prepared):
        commit_entered.set()
        return real_commit(conn, prepared)

    monkeypatch.setattr(service, "commit_prepared", observed_commit)

    async def scenario():
        async with _AsyncClient(app) as client:
            # 基线两个已提交批次：A->B, B->C（版本 2，3 个样本，2 条边）
            assert (await _upload(client, ("A", "B"))).status_code == 200
            assert (await _upload(client, ("B", "C"))).status_code == 200

            with _WriteLock(db_path):
                upload = asyncio.create_task(_upload(client, ("C", "D")))
                # 上传必须真的进入提交阶段并在写锁上等待，而不是已经完成
                await _wait_for(commit_entered)
                assert not upload.done()

                t0 = time.perf_counter()
                try:
                    affected, batches, graph, health = await asyncio.wait_for(
                        asyncio.gather(
                            client.get("/api/v1/samples/A/affected"),
                            client.get("/api/v1/batches"),
                            client.get("/api/v1/graph"),
                            client.get("/api/v1/health"),
                        ),
                        timeout=10.0,
                    )
                except TimeoutError:
                    raise AssertionError(
                        "写锁未释放时查询被上传阻塞（查询与提交共享了执行资源）"
                    ) from None
                elapsed_ms = (time.perf_counter() - t0) * 1000

                # 写锁仍被外部事务持有，上传仍未提交
                assert not upload.done()
                assert commit_entered.is_set()

                assert affected.status_code == 200, affected.text
                body = affected.json()
                # 完整已提交状态：只含 A->B->C，绝不含未提交的 C->D
                assert body["graph_version"] == 2
                assert body["affected_samples"] == ["B", "C"]

                assert batches.status_code == 200
                assert batches.json()["graph_version"] == 2
                assert [b["id"] for b in batches.json()["batches"]] == [2, 1]

                assert graph.status_code == 200
                assert graph.json()["graph_version"] == 2
                assert graph.json()["edge_count"] == 2
                assert graph.json()["sample_count"] == 3

                assert health.status_code == 200
                assert health.json() == {
                    "status": "ok",
                    "graph_version": 2,
                    "sample_count": 3,
                    "edge_count": 2,
                }
                # 查询不得因为提交线程卡在写锁上而排队
                assert elapsed_ms < 1500, f"查询耗时 {elapsed_ms:.0f}ms"

                # 锁释放后，原先等待的合法批次继续提交
                lock_holder.release()

            receipt = await asyncio.wait_for(upload, timeout=15.0)
            assert receipt.status_code == 200, receipt.text
            assert receipt.json()["inserted_edges"] == 1
            assert receipt.json()["graph_version"] == 3

            final = (await client.get("/api/v1/graph")).json()
            assert final["graph_version"] == 3
            assert final["edge_count"] == 3
            trace = (await client.get("/api/v1/samples/A/affected")).json()
            assert trace["affected_samples"] == ["B", "C", "D"]

    async def run():
        nonlocal lock_holder
        lock_holder = _WriteLock(db_path)
        await scenario()

    lock_holder = None
    asyncio.run(run())


def test_upload_capacity_exhaustion_does_not_starve_queries(tmp_path, monkeypatch):
    """并发上传数超过准入上限：仅异步排队；查询不受影响；解锁后全部合法提交。

    这里的上传数刻意大于任何“通用默认线程池”容量（默认池大小为
    CPU 核数 + 4），因此若上传与查询共用线程池，查询必被耗尽而超时。
    """
    uploads_count = os.cpu_count() + 10
    capacity = 4
    app, db_path = _load_app(
        tmp_path, monkeypatch,
        LINEAGE_UPLOAD_WORKERS=2,
        LINEAGE_COMMIT_WORKERS=1,
        LINEAGE_QUERY_WORKERS=2,
        LINEAGE_MAX_INFLIGHT_UPLOADS=capacity,
    )

    async def scenario():
        async with _AsyncClient(app) as client:
            assert (await _upload(client, ("ROOT", "BASELINE"))).status_code == 200

            with _WriteLock(db_path):
                # 超过准入上限的上传只在事件循环里排队，不占用工作线程
                uploads = [
                    asyncio.create_task(
                        _upload(client, (f"U{i}", f"V{i}"))
                    )
                    for i in range(uploads_count)
                ]
                await asyncio.sleep(0.4)
                # 写锁未释放：没有任何上传可以完成，但也没有被拒收
                assert all(not task.done() for task in uploads)
                assert app.state.executors.max_inflight_uploads == capacity

                t0 = time.perf_counter()
                try:
                    probes = await asyncio.wait_for(
                        asyncio.gather(*[
                            client.get("/api/v1/samples/ROOT/affected")
                            for _ in range(6)
                        ], client.get("/api/v1/batches"),
                            client.get("/api/v1/health")),
                        timeout=10.0,
                    )
                except TimeoutError:
                    raise AssertionError(
                        "大量上传等待写锁时查询被饿死（执行容量被上传耗尽）"
                    ) from None
                elapsed_ms = (time.perf_counter() - t0) * 1000

                for probe in probes:
                    assert probe.status_code == 200, probe.text
                for probe in probes[:6]:
                    assert probe.json()["graph_version"] == 1
                    assert probe.json()["affected_samples"] == ["BASELINE"]
                assert probes[6].json()["graph_version"] == 1
                assert [b["id"] for b in probes[6].json()["batches"]] == [1]
                assert probes[7].json()["edge_count"] == 1
                assert elapsed_ms < 1500, f"查询耗时 {elapsed_ms:.0f}ms"

                lock_holder.release()

            results = await asyncio.wait_for(
                asyncio.gather(*uploads, return_exceptions=True), timeout=60.0
            )
            for result in results:
                assert not isinstance(result, Exception), result
                assert result.status_code == 200, result.text
                assert result.json()["inserted_edges"] == 1

            final = (await client.get("/api/v1/graph")).json()
            # 基线 1 + 全部合法批次，全部提交成功，无人被拒
            assert final["graph_version"] == uploads_count + 1
            assert final["edge_count"] == uploads_count + 1
            batches = (await client.get("/api/v1/batches")).json()
            assert len(batches["batches"]) == uploads_count + 1

    async def run():
        nonlocal lock_holder
        lock_holder = _WriteLock(db_path)
        await scenario()

    lock_holder = None
    asyncio.run(run())


def test_slow_validation_does_not_block_other_commits(tmp_path, monkeypatch):
    """慢校验期间，其他批次的校验与提交仍可完成（不整体串行化）。"""
    app, _ = _load_app(
        tmp_path, monkeypatch,
        LINEAGE_UPLOAD_WORKERS=4,
        LINEAGE_COMMIT_WORKERS=1,
        LINEAGE_QUERY_WORKERS=2,
        LINEAGE_MAX_INFLIGHT_UPLOADS=8,
    )
    from app import service

    real_prepare = service.prepare_batch
    slow_started = threading.Event()

    def slow_prepare(conn, new_edges, received_rows):
        if ("SLOW", "TAIL") in new_edges:
            slow_started.set()
            time.sleep(1.0)
        return real_prepare(conn, new_edges, received_rows)

    monkeypatch.setattr(service, "prepare_batch", slow_prepare)

    async def scenario():
        async with _AsyncClient(app) as client:
            slow = asyncio.create_task(_upload(client, ("SLOW", "TAIL")))
            await _wait_for(slow_started, timeout=5.0)
            assert not slow.done()

            t0 = time.perf_counter()
            fast = await asyncio.wait_for(
                _upload(client, ("FAST", "LEAF")), timeout=2.5
            )
            fast_ms = (time.perf_counter() - t0) * 1000
            assert fast.status_code == 200, fast.text
            # 快批在慢批校验尚未结束时已完成提交
            assert not slow.done(), "慢校验把其他批次的提交一并阻塞了"
            assert fast_ms < 900, f"快批提交耗时 {fast_ms:.0f}ms"

            slow_result = await asyncio.wait_for(slow, timeout=10.0)
            assert slow_result.status_code == 200, slow_result.text

            final = (await client.get("/api/v1/graph")).json()
            assert final["edge_count"] == 2
            assert final["graph_version"] == 2

    asyncio.run(scenario())


def test_concurrent_cycle_race_http_exactly_one_commits(tmp_path, monkeypatch):
    """HTTP 并发：分别合法、合并成环的两批恰好一批成功，拒收返回真实环路。"""
    app, _ = _load_app(
        tmp_path, monkeypatch,
        LINEAGE_UPLOAD_WORKERS=4,
        LINEAGE_COMMIT_WORKERS=2,
        LINEAGE_QUERY_WORKERS=2,
        LINEAGE_MAX_INFLIGHT_UPLOADS=8,
    )

    async def scenario():
        async with _AsyncClient(app) as client:
            for i in range(8):
                p, q, r = (f"P{i}", f"Q{i}", f"R{i}")
                base = await _upload(client, (p, q))
                assert base.status_code == 200

                left, right = await asyncio.gather(
                    _upload(client, (q, r)),   # 单独合法
                    _upload(client, (r, p)),   # 单独合法，与另一批合并成环
                )
                statuses = sorted([left.status_code, right.status_code])
                assert statuses == [200, 409], (
                    f"第 {i} 轮应恰好 200+409，实际 {statuses}"
                )

                rejected = left if left.status_code == 409 else right
                accepted = right if rejected is left else left
                payload = rejected.json()
                assert payload["error"] == "cycle_rejected"
                cycle = payload["context"]["cycle"]
                assert cycle[0] == cycle[-1]
                assert set(cycle[:-1]) == {p, q, r}
                # 环路在“已提交图 + 被拒批次”上真实存在；被拒批次的边不入库，
                # 因此环路中恰有一条边不在最终图里
                graph = (await client.get("/api/v1/graph")).json()
                edges = {
                    (e["parent_sample"], e["child_sample"])
                    for e in graph["edges"]
                }
                cycle_edges = set(itertools.pairwise(cycle))
                assert len(cycle_edges - edges) == 1
                assert accepted.json()["inserted_edges"] == 1

            graph = (await client.get("/api/v1/graph")).json()
            assert graph["graph_version"] == 8 * 2
            assert graph["edge_count"] == 8 * 2
            # 全图始终无环
            from app.graph import build_graph, find_cycle
            g = build_graph(edges)
            assert find_cycle(g) is None

    asyncio.run(scenario())


def test_executor_pools_are_bounded_and_isolated(tmp_path, monkeypatch):
    """执行资源显式上限：三类池彼此独立，大小取自配置且固定。"""
    app, _ = _load_app(
        tmp_path, monkeypatch,
        LINEAGE_UPLOAD_WORKERS=3,
        LINEAGE_COMMIT_WORKERS=1,
        LINEAGE_QUERY_WORKERS=2,
        LINEAGE_MAX_INFLIGHT_UPLOADS=5,
    )

    async def scenario():
        async with app.router.lifespan_context(app):
            ex = app.state.executors
            assert ex.upload._max_workers == 3
            assert ex.commit._max_workers == 1
            assert ex.query._max_workers == 2
            assert ex.max_inflight_uploads == 5
            assert len({id(ex.upload), id(ex.commit), id(ex.query)}) == 3

    asyncio.run(scenario())


def test_docs_expose_upload_trace_and_batch_views(tmp_path, monkeypatch):
    """交互式文档（Swagger UI）可用的三个核心操作均在 schema 中。"""
    app, _ = _load_app(tmp_path, monkeypatch)

    async def scenario():
        async with _AsyncClient(app) as client:
            assert (await client.get("/docs")).status_code == 200
            schema = (await client.get("/openapi.json")).json()
            op = schema["paths"]["/api/v1/batches"]["post"]
            assert op["requestBody"]["content"]["multipart/form-data"]
            assert "/api/v1/samples/{sample_code}/affected" in schema["paths"]
            assert "/api/v1/batches" in schema["paths"]

    asyncio.run(scenario())
