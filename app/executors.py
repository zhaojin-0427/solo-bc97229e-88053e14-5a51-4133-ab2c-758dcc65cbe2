"""执行资源：彼此隔离、大小固定的线程池 + 有上限的上传准入。

需求约束的落点：

- 上传等待 SQLite 写锁时占用的是“提交池”，而不是通用默认线程池；
- 追溯 / 批次列表 / 全图 / 健康检查只用“查询池”，与上传/提交互不共享线程，
  因此即使提交线程全部卡在写锁等待上，查询仍能读到完整的已提交状态；
- 三个池的大小由配置显式给出，总量有上限；不靠调大线程池来掩盖问题；
- 超出准入上限的上传在事件循环中异步排队（``await`` 信号量），不占线程、
  不会被拒收，锁释放后仍会按提交时谱系重新校验并提交。
"""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

from . import config


class Executors:
    """服务生命周期内固定的执行器集合；进程内唯一，由 lifespan 创建/关闭。"""

    def __init__(self, *, upload: ThreadPoolExecutor | None = None,
                 commit: ThreadPoolExecutor | None = None,
                 query: ThreadPoolExecutor | None = None,
                 max_inflight_uploads: int | None = None) -> None:
        upload_workers = config.UPLOAD_WORKERS
        commit_workers = config.COMMIT_WORKERS
        query_workers = config.QUERY_WORKERS
        self.upload = upload or ThreadPoolExecutor(
            max_workers=upload_workers, thread_name_prefix="lineage-upload"
        )
        self.commit = commit or ThreadPoolExecutor(
            max_workers=commit_workers, thread_name_prefix="lineage-commit"
        )
        self.query = query or ThreadPoolExecutor(
            max_workers=query_workers, thread_name_prefix="lineage-query"
        )
        self.max_inflight_uploads = (
            max_inflight_uploads
            if max_inflight_uploads is not None
            else config.MAX_INFLIGHT_UPLOADS
        )
        self._upload_slots: asyncio.Semaphore | None = None

    # ---- 事件循环绑定 -------------------------------------------------

    def bind(self, loop: asyncio.AbstractEventLoop) -> None:
        """在应用启动时创建与当前事件循环绑定的原语。"""
        self._upload_slots = asyncio.Semaphore(self.max_inflight_uploads)

    def upload_slot(self) -> asyncio.Semaphore:
        if self._upload_slots is None:  # 防御：未 bind 时按上限即时创建
            self._upload_slots = asyncio.Semaphore(self.max_inflight_uploads)
        return self._upload_slots

    # ---- 关闭 ---------------------------------------------------------

    def shutdown(self, *, wait: bool = True) -> None:
        for pool in (self.upload, self.commit, self.query):
            pool.shutdown(wait=wait)

    # ---- 异步分派 -----------------------------------------------------

    async def run_upload(self, fn, *args):
        """上传侧（解析、快照、库外校验）工作。"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.upload, fn, *args)

    async def run_commit(self, fn, *args):
        """提交侧（短写事务）工作；写锁等待只发生在此池的线程上。"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.commit, fn, *args)

    async def run_query(self, fn, *args):
        """只读查询工作。"""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.query, fn, *args)
