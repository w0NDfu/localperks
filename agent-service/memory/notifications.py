"""One PostgreSQL listener per instance; durable events remain the source of truth."""
import asyncio
import hashlib
import threading
from contextlib import asynccontextmanager

import psycopg
from psycopg import sql


def channel(schema):
    return 'agent_events_' + hashlib.md5(schema.encode(), usedforsecurity=False).hexdigest()[:16]


class EventHub:
    def __init__(self, store):
        self.store = store
        self.queue = asyncio.Event()
        self.listeners = {}
        self.stopping = threading.Event()
        self.ready = threading.Event()

    async def start(self):
        self.loop = asyncio.get_running_loop()
        self.thread = threading.Thread(target=self._listen, name='agent-events', daemon=True)
        self.thread.start()
        if not await asyncio.to_thread(self.ready.wait, 5):
            await self.close()
            raise RuntimeError('PostgreSQL event listener did not become ready')

    def _listen(self):
        while not self.stopping.is_set():
            try:
                with psycopg.connect(self.store.dsn, autocommit=True, connect_timeout=3) as conn:
                    conn.execute(sql.SQL('LISTEN {}').format(sql.Identifier(channel(self.store.schema))))
                    self.ready.set()
                    self.loop.call_soon_threadsafe(self._wake, None)
                    while not self.stopping.is_set():
                        for event in conn.notifies(timeout=1, stop_after=1):
                            self.loop.call_soon_threadsafe(self._wake, event.payload)
            except (psycopg.Error, RuntimeError):
                self.stopping.wait(.5)

    def _wake(self, task_id):
        self.queue.set()
        groups = self.listeners.values() if task_id is None else [self.listeners.get(task_id, set())]
        for group in groups:
            for event in tuple(group):
                event.set()

    @asynccontextmanager
    async def watch(self, task_id):
        event = asyncio.Event()
        self.listeners.setdefault(task_id, set()).add(event)
        try:
            yield event
        finally:
            group = self.listeners[task_id]
            group.discard(event)
            if not group:
                self.listeners.pop(task_id, None)

    async def close(self):
        self.stopping.set()
        if hasattr(self, 'thread'):
            await asyncio.to_thread(self.thread.join, 4)
