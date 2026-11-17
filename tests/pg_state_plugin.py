"""Explicit test-only PostgreSQL schemas. No local storage fallback and no paid HTTP."""
import os
import uuid

import httpx
import psycopg
import pytest
from psycopg import sql


@pytest.fixture(autouse=True)
def isolated_postgres_state(monkeypatch):
    dsn = os.environ.get('AGENT_TEST_STATE_DSN')
    if not dsn:
        pytest.fail('Use the isolated persistence runner; AGENT_TEST_STATE_DSN is required')
    schema = 'test_' + uuid.uuid4().hex
    monkeypatch.setenv('AGENT_STATE_DSN', dsn)
    monkeypatch.setenv('AGENT_STATE_SCHEMA', schema)
    from memory.task_store import TaskStore
    original = TaskStore.__init__
    stores = []

    def tracked(self, *args, **kwargs):
        original(self, *args, **kwargs)
        stores.append(self)

    monkeypatch.setattr(TaskStore, '__init__', tracked)

    async def reject(self, request):
        raise AssertionError('Regression requires injected HTTP transports; paid/external calls are disabled')

    monkeypatch.setattr(httpx.AsyncHTTPTransport, 'handle_async_request', reject)
    try:
        yield
    finally:
        for store in stores:
            store.close()
        with psycopg.connect(dsn) as conn:
            conn.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
