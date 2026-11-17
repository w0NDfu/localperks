import os
import asyncio
import json
import time
import uuid

import httpx
import pytest

from agents.business import OrderAgent
from api.actions import ActionConfirmation, ActionRequest, ActionService
from core.config import Settings
from core.context import ToolContext
from core.state import AgentError
from main import create_app
from memory.task_store import TaskStore
from tools.executor import ToolExecutor
from tools.java_api import JavaAPI
from tools.registry import registry
from tests_support import TestMemory
from test_platform_p0 import shop, voucher


def test_task_survives_restart_and_redis_expiry_then_inherits_changed_constraints(tmp_path):
    async def check():
        db = tmp_path / 'tasks.sqlite3'
        settings = Settings(api_key='', state_dsn=os.environ['AGENT_TEST_STATE_DSN'])
        def java(request):
            path = request.url.path
            data = [{'id': 1, 'name': '美食'}] if path == '/shop-type/list' else [shop(1)] if path == '/shop/of/type' else [voucher()]
            return httpx.Response(200, json={'success': True, 'data': data})
        async def with_app(memory, action):
            app = create_app(transport=httpx.MockTransport(java), settings=settings, memory=memory)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                    return await action(client)
        async def first(client):
            result = await client.post('/agent/chat', json={'message': '推荐附近两个人吃饭、人均100元、今晚最划算三家'})
            assert result.json()['state'] == 'waiting_input'
            return result.json()
        initial = await with_app(TestMemory(), first)
        async def second(client):
            result = await client.post(f"/agent/tasks/{initial['task_id']}/resume", json={'message': '120.16,30.25 2090-11-05 19:00，预算改成80元'})
            assert result.status_code == 200, result.text
            value = result.json()
            assert value['task_id'] == initial['task_id']
            assert value['constraints']['people'] == 2 and value['constraints']['budget_per_person'] == 80
            assert value['state'] == 'completed'
            task = (await client.get('/agent/tasks/' + initial['task_id'])).json()
            assert 'ranking' in task['checkpoints']
            logs = (await client.get(f"/agent/tasks/{initial['task_id']}/events")).json()['events']
            assert any(e['type'] == 'workflow_started' for e in logs)
        await with_app(TestMemory(), second)
        store = TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        assert (await store.get('guest', initial['task_id']))['state'] == 'completed'
        with pytest.raises(AgentError, match='不属于'):
            await store.get('user:another', initial['task_id'])
    asyncio.run(check())


def test_atomic_task_claim_and_interruption_recovery(tmp_path):
    async def check():
        store = TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        task = await store.create('user:1', str(uuid.uuid4()), {'message': 'test'})
        await store.claim('user:1', task, lease_seconds=10)
        with pytest.raises(AgentError) as busy:
            await store.claim('user:1', task)
        assert busy.value.code == 'TASK_BUSY'
        await store.call(lambda c: c.execute('UPDATE agent_tasks SET lease_until=%s WHERE id=%s', (time.time() - 1, task)))
        assert (await store.get('user:1', task))['state'] == 'interrupted'
        await store.claim('user:1', task)
        await store.event(task, {'type': 'test', 'authorization': 'private', 'api_key': 'private', 'message': 'sk-01234567890123456789'})
        logs = await store.events('user:1', task)
        assert 'private' not in json.dumps(logs) and 'sk-01234567890123456789' not in json.dumps(logs)
    asyncio.run(check())


def test_checkpoint_recovery_reuses_static_node_but_refreshes_live_business_data(tmp_path):
    async def check():
        from orchestration.graph import TaskGraph
        from orchestration.state import TaskNode, TaskPlan, TaskResult
        store = TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        task = await store.create('guest', str(uuid.uuid4()), {'message': 'recommend'})
        calls = {'parse': 0, 'live': 0}
        plan = TaskPlan(nodes=[TaskNode(id='parse', agent='supervisor', operation='parse', volatile=False),
                               TaskNode(id='live', agent='shop', operation='read', dependencies=['parse'], volatile=True)])
        async def handler(node, _results):
            calls[node.id] += 1
            return {'generation': calls[node.id]}
        async def save(node, result):
            await store.checkpoint(task, node, result)
        await TaskGraph().run(plan, handler, checkpoint=save)
        reopened = TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        restored = {key: TaskResult.model_validate(value) for key, value in (await reopened.checkpoints('guest', task)).items()}
        await TaskGraph().run(plan, handler, checkpoint=save, restored=restored)
        assert calls == {'parse': 1, 'live': 2}
    asyncio.run(check())


@pytest.mark.parametrize('ambiguous', [False, True])
def test_confirmed_transaction_is_owned_idempotent_and_ambiguous_post_is_not_retried(tmp_path, ambiguous):
    async def check():
        writes = []
        def java(request):
            assert request.headers.get('authorization') in (None, 'login-one')
            if request.method == 'POST':
                writes.append(request.url.path)
                if ambiguous:
                    raise httpx.ReadTimeout('lost reply', request=request)
                data = {'orderId': '123', 'statusCode': 10, 'statusText': '排队中'}
            elif request.url.path == '/voucher/market':
                data = [voucher(id=8)]
            else:
                data = {'records': [{'orderId': '123', 'voucherId': 8, 'statusCode': 10}], 'total': 1}
            return httpx.Response(200, json={'success': True, 'data': data})
        store = TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        service = ActionService(store, Settings(api_key=''))
        async with httpx.AsyncClient(base_url='http://java.test', transport=httpx.MockTransport(java)) as client:
            ctx = ToolContext(java=JavaAPI(client, 'login-one'), authenticated=True, scope='user:1')
            request = ActionRequest(operation='seckill', voucher_id=8)
            prepared = await service.prepare(request, ctx)
            assert not writes and prepared['state'] == 'prepared'
            assert (await service.prepare(request, ctx))['action_id'] == prepared['action_id']
            with pytest.raises(AgentError):
                await service.prepare(ActionRequest(operation='seckill', voucher_id=9, idempotency_key=request.idempotency_key), ctx)
            other = ToolContext(java=JavaAPI(client, 'login-two'), authenticated=True, scope='user:2')
            with pytest.raises(AgentError) as denied:
                await service.get(prepared['action_id'], other)
            assert denied.value.status_code == 404
            confirmation = ActionConfirmation(confirmed=True, confirmation_token=prepared['confirmation_token'])
            result = await service.confirm(prepared['action_id'], confirmation, ctx)
            assert result['state'] == ('unknown' if ambiguous else 'completed')
            if ambiguous:
                with pytest.raises(AgentError):
                    await service.confirm(prepared['action_id'], confirmation, ctx)
                result = await service.reconcile(prepared['action_id'], ctx)
                assert result['result']['reconciled']
            repeated = await service.confirm(prepared['action_id'], confirmation, ctx)
            assert repeated['state'] == 'completed'
            assert writes == ['/voucher-order/seckill/8']
            logs = await service.events(prepared['action_id'], ctx)
            assert len([e for e in logs if e['type'] == 'submission_started']) == 1
            if ambiguous:
                assert any(e['type'] == 'reconciliation' and e['resolved'] for e in logs)
    asyncio.run(check())


def test_guest_and_mock_payment_and_false_confirmation_are_rejected(tmp_path):
    async def check():
        service = ActionService(TaskStore(os.environ['AGENT_TEST_STATE_DSN']), Settings(api_key=''))
        async with httpx.AsyncClient(base_url='http://java.test') as client:
            with pytest.raises(AgentError) as guest:
                await service.prepare(ActionRequest(operation='seckill', voucher_id=8), ToolContext(java=JavaAPI(client)))
            assert guest.value.status_code == 401
            with pytest.raises(AgentError) as pay:
                await service.prepare(ActionRequest(operation='mock_pay', order_id='123'), ToolContext(java=JavaAPI(client), authenticated=True, scope='user:1'))
            assert pay.value.code == 'MOCK_PAY_DISABLED'
    asyncio.run(check())
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        ActionConfirmation(confirmed=False, confirmation_token='x' * 32)
    with pytest.raises(ValidationError):
        ActionRequest(operation='cancel', order_id='123', user_id=2)


def test_order_tracker_uses_own_token_and_reports_queue_processing_pending():
    async def check():
        statuses = iter([10, 11, 12])
        def java(request):
            assert request.headers['authorization'] == 'owner-login'
            return httpx.Response(200, json={'success': True, 'data': {'orderId': '123', 'statusCode': next(statuses)}})
        async with httpx.AsyncClient(base_url='http://java.test', transport=httpx.MockTransport(java)) as client:
            events = []
            async def emit(event):
                events.append(event)
            result = await OrderAgent(ToolExecutor(registry)).track('123', ToolContext(java=JavaAPI(client, 'owner-login'), authenticated=True, scope='user:1'), emit, seconds=1, interval=.001)
            assert result['statusCode'] == 12
            assert [e['status'] for e in events if e['type'] == 'order_status'] == ['排队中', '处理中', '待支付']
    asyncio.run(check())


def test_chat_transaction_target_uses_explicit_entity_id_not_budget_number(tmp_path):
    async def check():
        from orchestration.supervisor import Supervisor
        calls=[]
        def java(request):
            calls.append(request.url.path)
            assert request.method=='GET'
            return httpx.Response(200,json={'success':True,'data':[voucher(id=8)]})
        cfg=Settings(api_key='',state_dsn=os.environ['AGENT_TEST_STATE_DSN'])
        store=TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        async with httpx.AsyncClient(base_url='http://java.test',transport=httpx.MockTransport(java)) as client:
            ctx=ToolContext(java=JavaAPI(client,'owner'),authenticated=True,scope='user:1')
            async def emit(_event):
                pass
            output=await Supervisor(cfg,store=store).run('预算100元，帮我抢8号券',[],ctx,emit)
            assert output['action']['preview']['voucher_id']==8
            assert calls==['/voucher/market']
    asyncio.run(check())
