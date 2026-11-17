import os
import asyncio

import httpx
import pytest

from api.actions import ActionConfirmation, ActionRequest, ActionService
from core.config import Settings
from core.context import ToolContext
from core.state import AgentError
from memory.task_store import TaskStore
from support import BusinessFixture, app_for
from tools.java_api import JavaAPI


@pytest.mark.parametrize('ambiguous', [False, True])
def test_two_concurrent_confirms_submit_exactly_once_and_reconcile_readonly(tmp_path, ambiguous):
    async def check():
        business = BusinessFixture()
        business.ambiguous = ambiguous
        store = TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        service = ActionService(store, Settings(api_key=''))
        async with httpx.AsyncClient(base_url='http://fixture', transport=httpx.MockTransport(business)) as client:
            ctx = ToolContext(java=JavaAPI(client, 'one'), authenticated=True, scope='user:1')
            action = await service.prepare(ActionRequest(operation='seckill', voucher_id=8), ctx)
            assert business.writes == 0
            confirmation = ActionConfirmation(confirmed=True, confirmation_token=action['confirmation_token'])
            responses = await asyncio.gather(*(service.confirm(action['action_id'], confirmation, ctx) for _ in range(2)), return_exceptions=True)
            assert business.writes == 1
            assert any(isinstance(r, dict) for r in responses)
            for response in responses:
                if isinstance(response, Exception):
                    assert isinstance(response, AgentError) and response.code in {'ACTION_BUSY', 'ACTION_RECONCILE_REQUIRED'}
            if ambiguous:
                with pytest.raises(AgentError):
                    await service.confirm(action['action_id'], confirmation, ctx)
                result = await service.reconcile(action['action_id'], ctx)
                assert result['result']['reconciled']
            result = await service.confirm(action['action_id'], confirmation, ctx)
            assert result['state'] == 'completed' and business.writes == 1
            logs = await service.events(action['action_id'], ctx)
            assert sum(e['type'] == 'submission_started' for e in logs) == 1
            row = await store.call(lambda c: c.execute('SELECT state FROM agent_actions WHERE id=%s', (action['action_id'],)).fetchone())
            assert row['state'] == 'completed'
    asyncio.run(check())


@pytest.mark.parametrize('change', ['price', 'stock', 'token', 'expired'])
def test_confirmation_revalidates_and_never_writes_after_conflict(tmp_path, change):
    async def check():
        business = BusinessFixture()
        store = TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        service = ActionService(store, Settings(api_key=''))
        async with httpx.AsyncClient(base_url='http://fixture', transport=httpx.MockTransport(business)) as client:
            ctx = ToolContext(java=JavaAPI(client, 'one'), authenticated=True, scope='user:1')
            action = await service.prepare(ActionRequest(operation='seckill', voucher_id=8), ctx)
            if change in ('price', 'stock'):
                business.offers[1][0]['payValue' if change == 'price' else 'stock'] = 6100 if change == 'price' else 0
            if change == 'expired':
                await store.call(lambda c: c.execute('UPDATE agent_actions SET created=0 WHERE id=%s', (action['action_id'],)))
            token = 'invalid-confirmation-token' if change == 'token' else action['confirmation_token']
            with pytest.raises(AgentError):
                await service.confirm(action['action_id'], ActionConfirmation(confirmed=True, confirmation_token=token), ctx)
            assert business.writes == 0
    asyncio.run(check())


def test_cross_user_and_expired_identity_cannot_resume_or_inspect(tmp_path):
    async def check():
        app = app_for(os.environ['AGENT_TEST_STATE_DSN'])
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://fixture') as client:
                first = (await client.post('/agent/chat', json={'message': '查询店铺1详情'}, headers={'authorization': 'one'})).json()
                task = first['task_id']
                for token, status in [('two', 404), ('expired', 401)]:
                    for suffix in ['', '/events']:
                        assert (await client.get(f'/agent/tasks/{task}{suffix}', headers={'authorization': token})).status_code == status
                    assert (await client.post(f'/agent/tasks/{task}/resume', json={'message': '恢复'}, headers={'authorization': token})).status_code == status
                # Java fixture denies the other user's order; Agent cannot turn denial into a successful fact.
                result = await client.post('/agent/chat', json={'message': '订单123状态'}, headers={'authorization': 'two'})
                assert result.status_code >= 400 or 'error' in str(result.json().get('trace', []))
    asyncio.run(check())
