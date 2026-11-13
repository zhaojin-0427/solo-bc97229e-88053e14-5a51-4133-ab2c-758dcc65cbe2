"""SQLite 连接管理与建表。

关键设置：
- WAL 日志：读（追溯查询/校验快照）与写（批次提交）不互相阻塞；
- 连接默认 autocommit（isolation_level=None），事务边界由服务层用
  BEGIN IMMEDIATE / COMMIT / ROLLBACK 显式控制；
- recursive_triggers 打开，保证外键级联等行为完整。
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from . import config


def connect(db_path: str | None = None) -> sqlite3.Connection:
    """打开一个连接并应用谱系服务所需的 PRAGMA。"""
    path = str(db_path or config.DB_PATH)
    # 数据目录与库文件所在目录都可能被单独覆盖，分别确保存在
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        path,
        timeout=config.BUSY_TIMEOUT_MS / 1000.0,
        isolation_level=None,  # autocommit：事务完全由代码显式管理
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    # WAL 让读事务不会被写事务阻塞（校验/追溯在写期间保持响应）
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA recursive_triggers=ON")
    conn.execute(f"PRAGMA busy_timeout={config.BUSY_TIMEOUT_MS}")
    return conn


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS samples (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    code       TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    received_rows   INTEGER NOT NULL,
    inserted_edges  INTEGER NOT NULL,
    duplicate_edges INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'committed',
    graph_version   INTEGER NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS edges (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER NOT NULL REFERENCES samples(id),
    child_id  INTEGER NOT NULL REFERENCES samples(id),
    batch_id  INTEGER NOT NULL REFERENCES batches(id),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(parent_id, child_id)
);

CREATE INDEX IF NOT EXISTS idx_edges_parent ON edges(parent_id);
CREATE INDEX IF NOT EXISTS idx_edges_child  ON edges(child_id);
"""


def init_db(conn: sqlite3.Connection) -> None:
    """幂等建表，并把图版本初始化为 0。"""
    conn.executescript(SCHEMA_SQL)
    conn.execute(
        "INSERT OR IGNORE INTO meta(key, value) VALUES ('graph_version', '0')"
    )


@contextmanager
def session(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    """请求级连接上下文：正常关闭，异常时回滚任何未完成事务。"""
    conn = connect(db_path)
    try:
        yield conn
    except Exception:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def get_graph_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT value FROM meta WHERE key = 'graph_version'"
    ).fetchone()
    return int(row["value"]) if row else 0
