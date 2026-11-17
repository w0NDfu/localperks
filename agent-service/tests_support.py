import copy
import uuid
from contextlib import asynccontextmanager
from core.state import AgentError


class TestMemory:
    __test__ = False
    def __init__(self):
        self.rows = {}

    @asynccontextmanager
    async def turn(self, scope, session):
        if session and (scope, session) not in self.rows:
            raise AgentError('SESSION_NOT_FOUND', '对话不属于当前用户', 404)
        session = session or str(uuid.uuid4())
        yield session, copy.deepcopy(self.rows.get((scope, session), []))

    async def save(self, scope, session, turns):
        self.rows[scope, session] = copy.deepcopy(turns)

    async def load(self, scope, session):
        if (scope, session) not in self.rows:
            raise AgentError('SESSION_NOT_FOUND', '对话不属于当前用户', 404)
        return copy.deepcopy(self.rows[scope, session])
