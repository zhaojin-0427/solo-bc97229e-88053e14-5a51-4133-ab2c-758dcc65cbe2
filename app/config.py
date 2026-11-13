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

# ---------------------------------------------------------------- 执行资源
#
# 三类工作使用彼此独立的固定大小线程池，总量显式有界：
#   - 上传校验池：CSV 解析 + 快照读取 + 库外图校验（CPU 密集，不持写锁）；
#   - 提交池：仅执行短写事务（SQLite 全库只有一个写者，串行化只发生在这里）；
#   - 查询池：追溯、批次列表、全图与健康检查等只读工作。
#
# 查询池与上传/提交池完全隔离：上传在等待 SQLite 写锁时只会占用提交线程，
# 不会占满查询线程。等待写锁的时长仍由 BUSY_TIMEOUT_MS 决定（不靠缩短它
# 来“腾出”查询资源）。
UPLOAD_WORKERS = max(1, int(os.environ.get("LINEAGE_UPLOAD_WORKERS", "4")))
COMMIT_WORKERS = max(1, int(os.environ.get("LINEAGE_COMMIT_WORKERS", "1")))
QUERY_WORKERS = max(1, int(os.environ.get("LINEAGE_QUERY_WORKERS", "4")))

# 已准入（已读取请求体、尚未完成）的上传上限。超出者在事件循环中异步排队，
# 不占用任何工作线程，也绝不因此拒收合法上传。
MAX_INFLIGHT_UPLOADS = max(
    1, int(os.environ.get("LINEAGE_MAX_INFLIGHT_UPLOADS", "8"))
)
