"""HTTP API 端到端测试（FastAPI TestClient + 临时 SQLite）。"""

from __future__ import annotations

import importlib

from fastapi.testclient import TestClient


def _make_client(tmp_path, monkeypatch):
    monkeypatch.setenv("LINEAGE_DB_PATH", str(tmp_path / "api_test.db"))
    monkeypatch.setenv("LINEAGE_DATA_DIR", str(tmp_path))
    from app import config, database, main
    importlib.reload(config)
    importlib.reload(database)
    importlib.reload(main)
    # 作为上下文管理器进入，触发 lifespan 完成建表与状态注入
    client = TestClient(main.app)
    client.__enter__()
    return client


def test_full_flow_upload_trace_and_docs(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)

    # 文档与演示页可访问
    assert client.get("/docs").status_code == 200
    assert client.get("/demo").status_code == 200
    assert client.get("/openapi.json").status_code == 200

    csv1 = (
        "亲本样本号,衍生样本号\n"
        "S001,S002\nS001,S003\nS002,S004\nS003,S004\n"
    ).encode()
    r = client.post(
        "/api/v1/batches",
        files={"file": ("batch1.csv", csv1, "text/csv")},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["inserted_edges"] == 4
    assert body["graph_version"] == 1

    # 追溯：S001 的全部下游，S004 只出现一次
    r = client.get("/api/v1/samples/S001/affected")
    assert r.status_code == 200
    data = r.json()
    assert data["affected_count"] == 3
    assert sorted(data["affected_samples"]) == ["S002", "S003", "S004"]
    assert data["graph_version"] == 1

    # 全图视图与健康检查
    assert client.get("/api/v1/graph").json()["edge_count"] == 4
    assert client.get("/api/v1/health").json()["status"] == "ok"


def test_english_headers_and_duplicate_collapse(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)
    csv_b = b"parent_sample,child_sample\nA,B\nA,B\nB,C\n"
    r = client.post("/api/v1/batches",
                    files={"file": ("b.csv", csv_b, "text/csv")})
    assert r.status_code == 200
    assert r.json()["inserted_edges"] == 2
    assert r.json()["duplicate_edges"] == 1


def test_cycle_upload_rejected_with_cycle_path(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)
    client.post("/api/v1/batches", files={"file": (
        "a.csv", "亲本样本号,衍生样本号\nA,B\nB,C\n".encode(), "text/csv")})

    r = client.post("/api/v1/batches", files={"file": (
        "cyc.csv", "亲本样本号,衍生样本号\nC,A\n".encode(), "text/csv")})
    assert r.status_code == 409
    payload = r.json()
    assert payload["error"] == "cycle_rejected"
    cycle = payload["context"]["cycle"]
    assert cycle[0] == cycle[-1]
    # 拒绝后图版本与边数不变
    overview = client.get("/api/v1/graph").json()
    assert overview["graph_version"] == 1 and overview["edge_count"] == 2


def test_self_loop_rejected(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)
    r = client.post("/api/v1/batches", files={"file": (
        "self.csv", "亲本样本号,衍生样本号\nZ9,Z9\n".encode(), "text/csv")})
    assert r.status_code == 409
    assert r.json()["context"]["cycle"] == ["Z9", "Z9"]
    assert client.get("/api/v1/graph").json()["edge_count"] == 0


def test_empty_field_rejected_422(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)
    r = client.post("/api/v1/batches", files={"file": (
        "bad.csv", "亲本样本号,衍生样本号\nA,   \n".encode(), "text/csv")})
    assert r.status_code == 422
    assert client.get("/api/v1/graph").json()["edge_count"] == 0


def test_bad_header_rejected_422(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)
    r = client.post("/api/v1/batches", files={"file": (
        "h.csv", b"foo,bar\nA,B\n", "text/csv")})
    assert r.status_code == 422


def test_unknown_sample_returns_found_false(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)
    r = client.get("/api/v1/samples/UNKNOWN/affected")
    assert r.status_code == 200
    assert r.json()["found"] is False


def test_bom_and_crlf_csv_supported(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)
    raw = "﻿亲本样本号,衍生样本号\r\nB1,B2\r\nB2,B3\r\n".encode()
    r = client.post("/api/v1/batches",
                    files={"file": ("bom.csv", raw, "text/csv")})
    assert r.status_code == 200, r.text
    trace = client.get("/api/v1/samples/B1/affected").json()
    assert trace["affected_samples"] == ["B2", "B3"]
    # 叶子节点：存在但无下游
    leaf = client.get("/api/v1/samples/B3/affected").json()
    assert leaf["found"] is True and leaf["affected_count"] == 0


def test_empty_and_non_utf8_files_rejected(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)
    r1 = client.post("/api/v1/batches",
                     files={"file": ("empty.csv", b"", "text/csv")})
    assert r1.status_code == 422
    r2 = client.post("/api/v1/batches", files={"file": (
        "raw.bin", b"\xff\xfe,bad\n", "application/octet-stream")})
    assert r2.status_code == 422
    assert client.get("/api/v1/graph").json()["edge_count"] == 0


def test_blank_lines_skipped(tmp_path, monkeypatch):
    client = _make_client(tmp_path, monkeypatch)
    raw = "亲本样本号,衍生样本号\nA,B\n\n\nB,C\n".encode()
    r = client.post("/api/v1/batches",
                    files={"file": ("blanks.csv", raw, "text/csv")})
    assert r.status_code == 200
    assert r.json()["received_rows"] == 2
