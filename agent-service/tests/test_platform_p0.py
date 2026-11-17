import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest
from pydantic import ValidationError

from core.config import Settings
from core.context import ToolContext
from decision.ranking import DecisionEngine
from main import create_app
from orchestration.graph import TaskGraph
from orchestration.planner import Planner
from orchestration.router import Router
from orchestration.state import ConsumptionConstraints, TaskNode, TaskPlan
from tools.executor import ToolExecutor
from tools.java_api import JavaAPI
from tools.registry import registry
from tests_support import TestMemory


def constraints(**changes):
    return ConsumptionConstraints.model_validate({
        'location': {'longitude': 120.16, 'latitude': 30.25}, 'people': 2,
        'budget_per_person': 100, 'visit_time': '2026-11-05T19:00:00+08:00', **changes})


def shop(id=1, **changes):
    return {'id': id, 'name': f'测试餐厅{id}', 'x': 120.1601, 'y': 30.2501,
            'avgPrice': 80, 'openHours': '10:00-22:00', **changes}


def voucher(id=1, **changes):
    return {'id': id, 'shopId': 1, 'type': 1, 'stock': 10, 'payValue': 6000, 'actualValue': 10000,
            'beginTime': '2020-01-01T00:00:00', 'endTime': '2099-01-01T00:00:00',
            'rules': '使用有效期2026-01-01至2026-12-31；全天可用；每桌限1张；2人可用', **changes}


def test_fast_router_and_constraint_inheritance():
    router = Router()
    assert router.route('查询店铺4详情').tool == 'shop_details'
    assert router.route('我的券包').tool == 'my_vouchers'
    assert router.route('订单123状态').arguments == {'order_id': '123'}
    planner = Planner()
    first = planner.constraints('附近两个人吃饭，人均100元以内，今晚推荐三家')
    assert set(planner.missing(first)) == {'location', 'visit_time'}
    second = planner.constraints('120.16,30.25 今晚19:00，预算改成80元', first.model_dump())
    assert second.budget_per_person == 80 and second.people == 2
    assert not planner.missing(second)


def test_finance_is_decimal_and_unknown_rules_are_not_verified():
    engine = DecisionEngine()
    now = datetime(2026, 11, 5, 16, tzinfo=ZoneInfo('Asia/Shanghai'))
    result = engine.rank([shop()], {1: [voucher()]}, constraints(), now=now)
    row = result['recommendations'][0]
    assert row['face_saving'] == 40 and row['estimated_cash_cost'] == 120
    assert row['estimated_per_person'] == 60
    result = engine.rank([shop()], {1: [voucher(rules='到店使用；使用规则以店铺说明为准')]}, constraints(), now=now)
    assert not result['recommendations'] and len(result['unverified_candidates']) == 1


@pytest.mark.parametrize('change', [{'stock': 0}, {'stock': None}, {'beginTime': '2099-01-01T00:00:00'}, {'rules': '使用有效期2025-01-01至2025-12-31；全天可用；每桌限1张'}])
def test_invalid_offers_are_excluded(change):
    result = DecisionEngine().rank([shop()], {1: [voucher(**change)]}, constraints(), now=datetime(2026, 11, 5, 16, tzinfo=ZoneInfo('Asia/Shanghai')))
    assert not result['recommendations'] and not result['unverified_candidates']


def test_unsupported_rules_are_not_assumed_and_purchase_day_is_respected():
    engine = DecisionEngine()
    now = datetime(2026, 11, 5, 16, tzinfo=ZoneInfo('Asia/Shanghai'))
    for extra in ['仅限会员', '满200元使用', '酒水不参与']:
        result = engine.rank([shop()], {1: [voucher(rules='长期有效；全天可用；每桌限1张；' + extra)]}, constraints(), now=now)
        assert not result['recommendations'] and result['unverified_candidates']
    result = engine.rank([shop()], {1: [voucher(rules='购买当天可用；全天可用；每桌限1张')]}, constraints(visit_time='2026-11-06T19:00:00+08:00'), now=now)
    assert not result['recommendations'] and not result['unverified_candidates']


def test_dag_parallel_branches_dependencies_failure_and_cycles():
    async def check():
        started, release = set(), asyncio.Event()
        async def handler(node, results):
            if node.id in {'a', 'b'}:
                started.add(node.id)
                if len(started) == 2:
                    release.set()
                await asyncio.wait_for(release.wait(), 1)
            if node.id == 'bad':
                raise RuntimeError('simulated')
            if node.id == 'join':
                assert results['a'].data == 'a' and results['b'].data == 'b'
            return node.id
        plan = TaskPlan(nodes=[TaskNode(id=n, agent='test', operation=n, dependencies=d) for n, d in [('a', []), ('b', []), ('join', ['a', 'b']), ('bad', []), ('skip', ['bad'])]])
        result = await TaskGraph(2).run(plan, handler)
        assert result['join'].status == 'completed' and result['skip'].status == 'skipped'
    asyncio.run(check())
    with pytest.raises(ValidationError):
        TaskPlan(nodes=[TaskNode(id='a', agent='test', operation='test', dependencies=['a'])])


def test_readonly_retry_and_fast_chat_never_contacts_model():
    async def check():
        calls = []
        def java(request):
            calls.append(request.url.path)
            if len(calls) == 1:
                raise httpx.ReadTimeout('simulated', request=request)
            return httpx.Response(200, json={'success': True, 'data': shop(4)})
        def llm(_request):
            pytest.fail('fast route must not call LLM')
        app = create_app(transport=httpx.MockTransport(java), model_transport=httpx.MockTransport(llm),
                         memory=TestMemory(), settings=Settings(api_key=''))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                response = await client.post('/agent/chat', json={'message': '查询店铺4详情'})
                assert response.status_code == 200, response.text
                assert response.json()['intent'] == 'simple'
                assert not any(e['type'] == 'model_request' for e in response.json()['trace'])
                assert len(calls) == 2
                missing = await client.post('/agent/chat', json={'message': '推荐附近两个人吃饭人均100今晚最划算三家'})
                assert missing.json()['state'] == 'waiting_input'
                assert 'location' in missing.json()['missing_fields']
    asyncio.run(check())


def test_complete_consumption_pipeline_uses_shop_then_parallel_voucher_tools():
    async def check():
        queried, simultaneous = [], set()
        gate = asyncio.Event()
        async def java(request):
            path = request.url.path
            queried.append(path)
            if path == '/shop-type/list':
                data = [{'id': 1, 'name': 'Food'}]  # actual local database label
            elif path == '/shop/of/type':
                data = [shop(i) for i in (1, 2, 3)]
            else:
                assert path.startswith('/voucher/list/')
                simultaneous.add(path)
                if len(simultaneous) == 3:
                    gate.set()
                await asyncio.wait_for(gate.wait(), 2)
                id = int(path.rsplit('/', 1)[-1])
                data = [voucher(id=id, shopId=id)]
            return httpx.Response(200, json={'success': True, 'data': data})
        app = create_app(transport=httpx.MockTransport(java), memory=TestMemory(), settings=Settings(api_key=''))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                response = await client.post('/agent/chat', json={'message': '推荐最划算三家', 'constraints': constraints(visit_time='2090-11-05T19:00:00+08:00').model_dump()})
                assert response.status_code == 200, response.text
                data = response.json()
                assert len(data['decision']['rejected']) == 3  # real rules do not cover 2090
                assert len(simultaneous) == 3
                assert any(e['type'] == 'plan' and any(n['dependencies'] for n in e['nodes']) for e in data['trace'])
    asyncio.run(check())
