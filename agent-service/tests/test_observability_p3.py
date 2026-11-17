import os
import asyncio
import json

import httpx

from core.config import Settings
from main import create_app
from memory.task_store import TaskStore
from observability.metrics import summarize
from evaluations.run import offline, percentile
from tests_support import TestMemory


def test_metrics_count_actual_calls_tokens_and_never_invent_billing():
    events = [
        {'type': 'model_request'}, {'type': 'model_response', 'usage': {'prompt_tokens': 10, 'completion_tokens': 3, 'total_tokens': 13}},
        {'type': 'tool_result', 'requests': [{'status': None}, {'status': 200}], 'result': []},
        {'type': 'rag', 'usage': {'total_tokens': 5}}, {'type': 'model_request'},
    ]
    result = summarize(events, 125)
    assert result['model_calls'] == 2 and result['total_tokens'] == 13
    assert result['usage_missing_calls'] == 1 and result['http_requests'] == 2
    assert result['embedding_tokens'] == 5 and result['cost']['currency_amount'] is None


def test_labelled_router_evaluation_passes_and_p95_is_nearest_rank():
    report = offline()
    assert report['intent_accuracy'] == 1, [row for row in report['rows'] if not row['passed']]
    assert report['tool_selection_accuracy'] == 1
    assert percentile([10, 20, 30, 40]) == 40


def test_task_and_logs_are_scoped_by_real_java_identity(tmp_path):
    async def check():
        def java(request):
            if request.url.path == '/user/me':
                data = {'id': 1 if request.headers['authorization'] == 'one' else 2}
            else:
                data = {'id': 4, 'name': '真实接口测试店', 'openHours': '08:00-22:00'}
            return httpx.Response(200, json={'success': True, 'data': data})
        app = create_app(transport=httpx.MockTransport(java), settings=Settings(api_key='', state_dsn=os.environ['AGENT_TEST_STATE_DSN']), memory=TestMemory())
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                result = (await client.post('/agent/chat', json={'message': '查询店铺4详情'}, headers={'authorization': 'one'})).json()
                task_id = result['task_id']
                assert result['metrics']['model_calls'] == 0 and result['metrics']['tool_calls'] == 1
                for path in [f'/agent/tasks/{task_id}', f'/agent/tasks/{task_id}/events']:
                    assert (await client.get(path, headers={'authorization': 'two'})).status_code == 404
                    owned = await client.get(path, headers={'authorization': 'one'})
                    assert owned.status_code == 200
                    assert 'authorization' not in owned.text
                forbidden = await client.post('/agent/chat', json={'message': '查看订单', 'user_id': 2}, headers={'authorization': 'one'})
                assert forbidden.status_code == 422
    asyncio.run(check())
