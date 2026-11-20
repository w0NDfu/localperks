"""A PostgreSQL conversation journal with a replaceable, versioned Redis cache."""
import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from redis.exceptions import RedisError

from memory.task_store import dump


CACHE_PUT = """
local old = redis.call('hget',KEYS[1],'revision')
local new = ARGV[1]
if old and (#old > #new or (#old == #new and old >= new)) then return 0 end
redis.call('hset',KEYS[1],'revision',new,'data',ARGV[2])
redis.call('expire',KEYS[1],86400)
return 1
"""


class ConversationMemory:
    def __init__(self, store, redis, *, observer=None):
        self.store, self.redis, self.observer = store, redis, observer
        self.wake = asyncio.Event()
        self.worker = None

    @property
    def rows(self):
        # An injected test observer is updated only from committed PG snapshots.
        return self.observer.rows if self.observer is not None else {}

    def key(self, scope, session):
        return f'agent:conversation:v2:{self.store.schema}:{scope}:{session}'

    async def start(self):
        self.worker = asyncio.create_task(self._run())

    async def close(self):
        if self.worker:
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)

    async def load(self, scope, session):
        # Validate scope and committed revision in PG before serving a cached
        # display snapshot. Workflow inputs are reloaded by TaskStore.claim.
        revision = await self.store.conversation_revision(scope, session)
        try:
            cached_revision, data = await self.redis.hmget(self.key(scope, session), 'revision', 'data')
            if cached_revision == str(revision) and data:
                turns = json.loads(data)
                if isinstance(turns, list):
                    return turns
        except (RedisError, ValueError, TypeError):
            pass
        return (await self.store.conversation_snapshot(scope, session))['turns']

    @asynccontextmanager
    async def turn(self, scope, session):
        new = session is None
        session = session or str(uuid.uuid4())
        snapshot = await self.store.conversation_snapshot(scope, session, create=new)
        yield session, snapshot['turns']
        # TaskStore.claim serializes execution and reloads history under the
        # session lock. No expiring Redis mutex is relied on for correctness.

    async def materialize(self, job):
        # Outbox stores identifiers/revision, not potentially stale history.
        snapshot = await self.store.conversation_snapshot(job['scope'], job['session_id'])
        await self.redis.eval(CACHE_PUT, 1, self.key(job['scope'], job['session_id']),
                              str(snapshot['revision']), dump(snapshot['turns']))
        if self.observer is not None:
            await self.observer.save(job['scope'], job['session_id'], snapshot['turns'])
        await self.store.acknowledge_cache(job, snapshot['revision'])

    async def flush(self):
        for job in await self.store.claim_cache_updates():
            try:
                await self.materialize(job)
            except asyncio.CancelledError:
                await asyncio.shield(self.store.retry_cache(job))
                raise
            except Exception:
                await self.store.retry_cache(job)

    async def after_commit(self):
        self.wake.set()
        # Test observers remain deterministic, without introducing a second
        # authoritative memory implementation into the workflow.
        if self.observer is not None:
            try:
                await self.flush()
            except Exception:
                pass

    async def _run(self):
        while True:
            try:
                self.wake.clear()
                await self.flush()
                await asyncio.wait_for(self.wake.wait(), 1)
            except TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                await asyncio.sleep(1)
