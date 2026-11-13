"""真实 HTTP 并发场景验证（手动脚本，非 pytest）。

场景一：已有 A->B，两个客户端同时提交
  批次 X: B->C  （单独合法）
  批次 Y: C->A  （单独合法，与 X 合并成环）
要求：恰好一批 200、一批 409，最终图无环。
同时在两个上传进行中反复发起追溯查询，要求全部快速成功。

场景二：外部连接持有 SQLite 写锁，同时发起多于准入上限的上传
要求：写锁未释放时追溯/批次/健康检查仍返回完整已提交状态；解锁后
      全部合法上传照常提交（无人被拒收）。写锁通过 LINEAGE_DB_PATH 定位，
      默认按容器内路径 /data/lineage.db，可用环境变量覆盖。
"""

from __future__ import annotations

import concurrent.futures
import os
import sqlite3
import time

import httpx

BASE = "http://127.0.0.1:8000"
DB_PATH = os.environ.get("LINEAGE_DB_PATH", "/data/lineage.db")


def post_batch(name: str, csv_text: str) -> tuple[str, int, dict]:
    files = {"file": (f"{name}.csv", csv_text.encode(), "text/csv")}
    t0 = time.perf_counter()
    resp = httpx.post(f"{BASE}/api/v1/batches", files=files, timeout=30)
    elapsed = (time.perf_counter() - t0) * 1000
    return name, resp.status_code, {"ms": round(elapsed, 1), **resp.json()}


def scenario_cycle_race() -> None:
    # 准备基线 A->B（每次运行用唯一样本号，避免历史数据干扰判断）
    stamp = str(int(time.time() * 1000))[-8:]
    a, b, c = f"A{stamp}", f"B{stamp}", f"C{stamp}"
    base = post_batch("base", f"亲本样本号,衍生样本号\n{a},{b}\n")
    print("baseline:", base)

    def worker(payload: tuple[str, str]) -> tuple[str, int, dict]:
        # 尽量让两个请求同时发出
        time.sleep(0.05)
        return post_batch(*payload)

    trace_latencies: list[float] = []
    stop = False

    def tracer() -> list[float]:
        lat: list[float] = []
        while not stop:
            t0 = time.perf_counter()
            r = httpx.get(f"{BASE}/api/v1/samples/{a}/affected", timeout=30)
            lat.append((time.perf_counter() - t0) * 1000)
            assert r.status_code == 200
        return lat

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        trace_fut = pool.submit(tracer)
        futs = [
            pool.submit(worker, ("batchX", f"亲本样本号,衍生样本号\n{b},{c}\n")),
            pool.submit(worker, ("batchY", f"亲本样本号,衍生样本号\n{c},{a}\n")),
        ]
        results = [f.result() for f in futs]
        stop = True
        trace_latencies = trace_fut.result()

    for name, code, body in results:
        print(f"result {name}: HTTP {code} {body}")

    statuses = sorted(code for _, code, _ in results)
    assert statuses == [200, 409], f"期望恰好 200+409，实际 {statuses}"
    rejected = next(body for _, code, body in results if code == 409)
    assert rejected["error"] == "cycle_rejected"
    cycle = rejected["context"]["cycle"]
    assert cycle[0] == cycle[-1] and set(cycle[:-1]) <= {a, b, c}

    graph = httpx.get(f"{BASE}/api/v1/graph", timeout=10).json()
    print("final edge_count:", graph["edge_count"], "version:", graph["graph_version"])

    # 追溯在并发期间保持响应：给出延迟分布
    trace_latencies.sort()
    p50 = trace_latencies[len(trace_latencies) // 2]
    p99 = trace_latencies[min(len(trace_latencies) - 1,
                              int(len(trace_latencies) * 0.99))]
    print(f"trace calls during uploads: n={len(trace_latencies)}, "
          f"p50={p50:.1f}ms, p99={p99:.1f}ms, max={max(trace_latencies):.1f}ms")
    assert len(trace_latencies) > 0


def scenario_lock_wait(count: int = 12) -> None:
    """写锁被外部事务持有时，查询不被拖住；解锁后合法上传全部提交。"""
    if not os.path.exists(DB_PATH):
        print(f"skip lock scenario: {DB_PATH} 不存在（请设置 LINEAGE_DB_PATH）")
        return

    stamp = str(int(time.time() * 1000))[-8:]
    root, leaf = f"R{stamp}", f"L{stamp}"
    _, base_status, base_body = post_batch(
        "base", f"亲本样本号,衍生样本号\n{root},{leaf}\n"
    )
    assert base_status == 200, base_body
    before = httpx.get(f"{BASE}/api/v1/health", timeout=10).json()
    print("baseline health:", before)

    def upload(i: int) -> int:
        return post_batch(
            f"U{i}", f"亲本样本号,衍生样本号\nU{stamp}_{i},V{stamp}_{i}\n"
        )[1]

    writer = sqlite3.connect(DB_PATH, isolation_level=None, timeout=0)
    writer.execute("BEGIN IMMEDIATE")
    # 故意写入但未提交：任何查询都不得看到它
    writer.execute("INSERT OR IGNORE INTO samples(code) VALUES ('__uncommitted__')")
    print("external write lock held")

    with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
        futs = [pool.submit(upload, i) for i in range(count)]
        time.sleep(0.5)

        # 写锁未释放：查询必须返回完整已提交状态
        for _ in range(3):
            t0 = time.perf_counter()
            trace = httpx.get(f"{BASE}/api/v1/samples/{root}/affected", timeout=10)
            batches = httpx.get(f"{BASE}/api/v1/batches", timeout=10)
            health = httpx.get(f"{BASE}/api/v1/health", timeout=10)
            elapsed = (time.perf_counter() - t0) * 1000
            for r in (trace, batches, health):
                assert r.status_code == 200, r.text
            body = trace.json()
            assert body["graph_version"] == before["graph_version"], body
            assert body["affected_samples"] == [leaf], body
            assert batches.json()["graph_version"] == before["graph_version"]
            assert health.json()["edge_count"] == before["edge_count"]
            print(f"query during lock: {elapsed:.0f}ms, version "
                  f"{body['graph_version']}（未看到未提交写入）")
            assert elapsed < 3000

        assert all(not f.done() for f in futs), "上传不应在写锁释放前提交"
        writer.execute("ROLLBACK")
        writer.close()
        print("lock released")

        statuses = [f.result() for f in futs]

    assert statuses == [200] * count, f"解锁后应全部提交，实际 {statuses}"
    after = httpx.get(f"{BASE}/api/v1/health", timeout=10).json()
    print("final health:", after)
    assert after["edge_count"] == before["edge_count"] + count
    assert after["graph_version"] == before["graph_version"] + count


def main() -> None:
    scenario_cycle_race()
    scenario_lock_wait()
    print("CONCURRENCY SCENARIO PASSED")


if __name__ == "__main__":
    main()
