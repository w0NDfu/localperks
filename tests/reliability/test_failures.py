import os
import asyncio

import httpx
import pytest

from core.config import Settings
from core.context import ToolContext
from core.state import AgentError
from main import create_app
from models.structured import json_completion
from rag.retrieval import RagService, Reranker
from rag.store import PgVectorStore
from tools.executor import ToolExecutor
from tools.java_api import JavaAPI
from tools.registry import registry
from support import BusinessFixture, completion


@pytest.mark.parametrize('failure,expected,attempts', [
    ('http500', 'SHOPLINK_HTTP_ERROR', 1), ('timeout', 'SHOPLINK_TIMEOUT', 2),
    ('connect', 'SHOPLINK_UNAVAILABLE', 2), ('business', 'SHOPLINK_BUSINESS_ERROR', 1),
    ('invalid', 'SHOPLINK_INVALID_RESPONSE', 1)])
def test_java_faults_have_bounded_read_retry_and_trace(failure, expected, attempts):
    async def check():
        calls, trace = [], []
        def injected(request):
            calls.append(request.url.path)
            if failure == 'timeout':
                raise httpx.ReadTimeout('injected', request=request)
            if failure == 'connect':
                raise httpx.ConnectError('injected', request=request)
            return httpx.Response(500) if failure == 'http500' else httpx.Response(200, json={} if failure == 'invalid' else {'success': False})
        async with httpx.AsyncClient(base_url='http://java.fixture', transport=httpx.MockTransport(injected)) as client:
            async def emit(event):
                trace.append(event)
            result = await ToolExecutor(registry).execute('shop', 'shop_details', {'shop_id': 1}, ToolContext(java=JavaAPI(client)), emit)
            assert result.status == 'failed' and result.error.code == expected
            assert len(calls) == attempts
            assert len(trace[-1]['requests']) == attempts
            assert trace[-1]['retries'] == attempts - 1
    asyncio.run(check())


@pytest.mark.parametrize('mode,expected', [('429', 'MODEL_HTTP_ERROR'), ('timeout', 'MODEL_TIMEOUT'),
                                          ('json', 'MODEL_INVALID_JSON'), ('array', 'MODEL_INVALID_JSON')])
def test_llm_faults_are_bounded_and_explicit(mode, expected):
    async def check():
        calls, trace = [], []
        def injected(request):
            calls.append(1)
            if mode == 'timeout':
                raise httpx.ReadTimeout('injected', request=request)
            return httpx.Response(429, json={'error': {'message': 'injected'}}) if mode == '429' else completion('bad-json' if mode == 'json' else '[]')
        async def emit(event):
            trace.append(event)
        with pytest.raises(AgentError) as error:
            await json_completion(Settings(api_key='fixture', model='fixture'), [{'role': 'user', 'content': 'test'}], emit,
                                  transport=httpx.MockTransport(injected))
        assert error.value.code == expected and len(calls) == 1
        assert trace[0]['type'] == 'model_request'
    asyncio.run(check())


def test_pgvector_outage_returns_explicit_error():
    async def check():
        # Closed loopback port, never stops or connects to the business pgvector instance.
        service = RagService(Settings(api_key=''), store=PgVectorStore('postgresql://fixture:fixture@127.0.0.1:1/fixture'))
        with pytest.raises(AgentError) as error:
            await service.search('订单取消规则')
        assert error.value.code == 'RAG_STORE_UNAVAILABLE'
    asyncio.run(check())


@pytest.mark.parametrize('mode', ['http500', 'invalid_index'])
def test_rerank_outage_or_invalid_output_keeps_rrf_order(mode):
    async def check():
        rows = [{'id': str(i), 'title': '规则', 'text': '规则证据'} for i in range(3)]
        def injected(_request):
            return httpx.Response(500) if mode == 'http500' else httpx.Response(200, json={'results': [{'index': 99, 'relevance_score': 1}]})
        ranked, backend, warning = await Reranker(Settings(api_key='fixture'), httpx.MockTransport(injected)).rank('规则', rows)
        assert ranked == rows and backend == 'rrf' and warning
    asyncio.run(check())


def test_cancelled_turn_persists_interruption_then_requeries(tmp_path):
    async def check():
        from tests_support import TestMemory
        from support import BusinessFixture
        entered = asyncio.Event()
        fixture = BusinessFixture()
        blocked = True
        async def java(request):
            if blocked and request.url.path == '/shop/1':
                entered.set()
                await asyncio.Event().wait()
            return await fixture(request)
        app = create_app(settings=Settings(api_key='', state_dsn=os.environ['AGENT_TEST_STATE_DSN']),
                         memory=TestMemory(), transport=httpx.MockTransport(java))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://fixture') as client:
                request = asyncio.create_task(client.post('/agent/chat', json={'message': '查询店铺1详情'}))
                await asyncio.wait_for(entered.wait(), 3)
                request.cancel()
                await asyncio.gather(request, return_exceptions=True)
                rows = await app.state.task_store.call(lambda c: c.execute('SELECT id,state FROM agent_tasks').fetchall())
                assert len(rows) == 1 and rows[0]['state'] == 'interrupted'
                blocked = False
                response = await client.post(f"/agent/tasks/{rows[0]['id']}/resume", json={'message': '恢复'})
                assert response.status_code == 200 and response.json()['state'] == 'completed'
                assert any(r['path'] == '/shop/1' for r in fixture.requests)
    asyncio.run(check())
