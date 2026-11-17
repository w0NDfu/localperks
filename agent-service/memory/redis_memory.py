import json
import uuid
from contextlib import asynccontextmanager

from redis.asyncio import Redis
from redis.exceptions import LockError, RedisError

from core.state import AgentError

TTL_SECONDS = 86400
MAX_TURNS = 8


class RedisMemory:
    def __init__(self, client: Redis):
        self.client = client

    def key(self, scope: str, session_id: str) -> str:
        return f"agent:conversation:{scope}:{session_id}"

    async def load(self, scope: str, session_id: str) -> list[dict]:
        try:
            saved = await self.client.get(self.key(scope, session_id))
        except RedisError as exc:
            raise AgentError("MEMORY_UNAVAILABLE", "对话存储不可用，请确认Redis已启动", 503) from exc
        if saved is None:
            raise AgentError("SESSION_NOT_FOUND", "对话已过期或不属于当前用户，请开始新对话", 404)
        return json.loads(saved)

    async def save(self, scope: str, session_id: str, turns: list[dict]):
        try:
            await self.client.set(self.key(scope, session_id),
                                  json.dumps(turns[-MAX_TURNS:], ensure_ascii=False), ex=TTL_SECONDS)
        except RedisError as exc:
            raise AgentError("MEMORY_UNAVAILABLE", "无法保存对话，请确认Redis可用", 503) from exc

    @asynccontextmanager
    async def turn(self, scope: str, session_id: str | None):
        existing = session_id is not None
        session_id = session_id or str(uuid.uuid4())
        key = self.key(scope, session_id)
        lock = self.client.lock(key + ":lock", timeout=120, blocking=False)
        acquired = False
        try:
            acquired = await lock.acquire()
            if not acquired:
                raise AgentError("SESSION_BUSY", "这段对话正在处理中，请等待本轮结束", 409)
            saved = await self.client.get(key)
            if saved is None:
                if existing:
                    raise AgentError("SESSION_NOT_FOUND", "对话已过期或不属于当前用户，请开始新对话", 404)
                turns = []
            else:
                turns = json.loads(saved)
            yield session_id, turns
        except RedisError as exc:
            raise AgentError("MEMORY_UNAVAILABLE", "对话存储不可用，请确认Redis已启动", 503) from exc
        finally:
            if acquired:
                try:
                    await lock.release()
                except (RedisError, LockError):
                    pass

