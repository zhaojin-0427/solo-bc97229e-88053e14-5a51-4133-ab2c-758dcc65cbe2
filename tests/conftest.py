"""pytest 夹具：每个测试使用独立临时数据库。"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture()
def temp_db(tmp_path, monkeypatch):
    db_path = tmp_path / "test_lineage.db"
    monkeypatch.setenv("LINEAGE_DB_PATH", str(db_path))
    monkeypatch.setenv("LINEAGE_DATA_DIR", str(tmp_path))
    # 配置模块在导入时读取环境变量，需重新加载
    from app import config, database
    importlib.reload(config)
    importlib.reload(database)
    conn = database.connect(str(db_path))
    database.init_db(conn)
    conn.close()
    yield database, db_path
