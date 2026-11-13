"""运行期配置，全部可通过环境变量覆盖。"""

from __future__ import annotations

import os
from pathlib import Path


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    return Path(raw) if raw else default


# SQLite 数据库文件目录（容器内默认 /data，由 Compose 挂载持久卷）
DATA_DIR = _env_path("LINEAGE_DATA_DIR", Path(__file__).resolve().parent.parent / "data")
DB_PATH = _env_path("LINEAGE_DB_PATH", DATA_DIR / "lineage.db")

# 等待写锁的秒数（WAL 下读写不互斥，仅写写排队）
BUSY_TIMEOUT_MS = int(os.environ.get("LINEAGE_BUSY_TIMEOUT_MS", "15000"))

# 乐观并发冲突（提交时发现版本已变）时，在库外重新校验并重试的最大次数
MAX_COMMIT_RETRIES = int(os.environ.get("LINEAGE_MAX_RETRIES", "50"))

# 单次上传允许的最大 CSV 字节数 / 行数，防止异常大文件拖垮服务
MAX_UPLOAD_BYTES = int(os.environ.get("LINEAGE_MAX_UPLOAD_BYTES", str(20 * 1024 * 1024)))
MAX_UPLOAD_ROWS = int(os.environ.get("LINEAGE_MAX_UPLOAD_ROWS", "200000"))
