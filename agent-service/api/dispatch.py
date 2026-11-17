"""Bounded per-instance dispatch; connections never own the workflow lifetime.

Credentials live only in the coroutine context. After a process restart clients
must reauthenticate and resubmit the same request ID; there is no unattended replay.
"""
import asyncio
from contextlib import asynccontextmanager

from core.state import AgentError


class Dispatcher:
    def __init__(self, concurrency=4, max_pending=128):
        self.limit = asyncio.Semaphore(concurrency)
        self.max_pending = max_pending
        self.tasks = {}
        self.sessions = {}
        self.admission = asyncio.Lock()

    @asynccontextmanager
    async def capacity(self):
        async with self.admission:
            # Reserve before persistence; overload must not leave orphan receipts.
            if len(self.tasks) >= self.max_pending:
                raise AgentError("SERVICE_BUSY", "客服任务队列已满，请稍后重试", 503)
            yield

    def submit(self, task_id, scope, session_id, run):
        if task_id in self.tasks:
            return
        key = (scope, session_id)
        lock, refs = self.sessions.get(key, (asyncio.Lock(), 0))
        self.sessions[key] = (lock, refs + 1)

        async def execute():
            try:
                # Waiting on the same session does not consume a worker slot.
                async with lock, self.limit:
                    await run()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Workflow persists a sanitized error; never log tokens or provider text.
                import logging
                logging.getLogger(__name__).warning("Customer workflow ended with an error")
            finally:
                self.tasks.pop(task_id, None)
                _, count = self.sessions[key]
                if count == 1:
                    self.sessions.pop(key)
                else:
                    self.sessions[key] = (lock, count - 1)

        self.tasks[task_id] = asyncio.create_task(execute())

    async def close(self):
        tasks = list(self.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
