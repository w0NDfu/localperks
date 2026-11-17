import asyncio
import copy
import json
import os
import uuid
from contextlib import asynccontextmanager

import httpx
import pytest

from core.config import Settings
from core.context import ToolContext
from core.state import AgentError
from main import create_app
from memory.redis_memory import RedisMemory
from tools.java_api import JavaAPI
from tools.registry import registry


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
        return self.rows[scope, session]


def completion(content=None, name=None, arguments=None):
    message = {'role': 'assistant', 'content': content}
    if name:
        message['tool_calls'] = [{'id': 'call_' + name, 'type': 'function',
                                 'function': {'name': name, 'arguments': json.dumps(arguments)}}]
    return httpx.Response(200, json={
        'id': 'chatcmpl-test', 'object': 'chat.completion', 'created': 1, 'model': 'test-model',
        'choices': [{'index': 0, 'message': message, 'finish_reason': 'tool_calls' if name else 'stop'}],
        'usage': {'prompt_tokens': 30, 'completion_tokens': 10, 'total_tokens': 40},
    })


def test_sdk_delegates_queries_java_and_continues_with_tool_results():
    async def check():
        calls, queries = [], []
        replies = [
            completion(name='delegate_shop', arguments={'task': '查咖啡店'}),
            completion(name='find_shops', arguments={'keyword': '咖啡'}),
            completion('店铺4：咖啡馆'),
            completion(name='delegate_voucher', arguments={'task': '查店铺4的秒杀券'}),
            completion(name='find_vouchers', arguments={'shop_id': 4, 'seckill_only': True}),
            completion('券13：29.9元，剩余7张'),
            completion('咖啡馆（店铺4）有券13，29.9元，剩余7张。'),
        ]
        def llm(request):
            body = json.loads(request.content)
            assert request.url.path == '/v1/chat/completions'
            assert request.headers['authorization'] == 'Bearer test-relay-key'
            assert body['model'] == 'test-model'
            assert 'java-private-token' not in request.content.decode()
            calls.append(body)
            return replies.pop(0)
        def java(request):
            queries.append(request.url.path)
            if request.url.path == '/user/me':
                assert request.headers['authorization'] == 'java-private-token'
                data = {'id': 9}
            else:
                assert 'authorization' not in request.headers
                data = [{'id': 4, 'name': '咖啡馆'}] if request.url.path == '/shop/of/name' else [
                    {'id': 13, 'shopId': 4, 'type': 1, 'title': '咖啡券', 'payValue': 2990,
                     'actualValue': 5000, 'stock': 7, 'beginTime': '2020-01-01T00:00:00', 'endTime': '2099-01-01T00:00:00'}]
            return httpx.Response(200, json={'success': True, 'data': data})
        memory = TestMemory()
        app = create_app(transport=httpx.MockTransport(java), model_transport=httpx.MockTransport(llm),
                         memory=memory, settings=Settings(orchestration_mode="legacy", api_key='test-relay-key', model='test-model'))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                result = await client.post('/agent/chat', json={'message': '查咖啡店的券'}, headers={'authorization': 'java-private-token'})
                assert result.status_code == 200, result.text
                body = result.json()
                assert '剩余7张' in body['answer']
                assert queries == ['/user/me', '/shop/of/name', '/voucher/market']
                assert len(calls) == 7 and not replies
                assert calls[2]['messages'][-1]['role'] == 'tool'
                tool = json.loads(calls[5]['messages'][-1]['content'])
                assert tool['vouchers'][0]['priceYuan'] == 29.9
                assert tool['vouchers'][0]['activityState'] == '可抢'
                assert len([e for e in body['trace'] if e['type'] == 'model_request']) == 7
                stored = list(memory.rows.values())[0]
                assert 'java-private-token' not in json.dumps(stored)
                other = await client.get('/agent/conversations/' + body['session_id'])
                assert other.status_code == 404
                own = await client.get('/agent/conversations/' + body['session_id'], headers={'authorization': 'java-private-token'})
                assert own.json()['turns'][0]['user'] == '查咖啡店的券'
    asyncio.run(check())


@pytest.mark.parametrize('arguments', ['{"page":0}', '{"page":"1"}', '{"url":"http://evil"}', 'not-json'])
def test_invalid_tool_arguments_never_contact_java(arguments):
    async def check():
        def unexpected(_request):
            pytest.fail('invalid tool contacted Java')
        async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as client:
            ctx = ToolContext(java=JavaAPI(client))
            result = await registry.execute('shop', 'find_shops', arguments, ctx)
            assert result['error']['code'] == 'INVALID_ARGUMENTS'
            denied = await registry.execute('shop', 'my_vouchers', '{}', ctx)
            assert denied['error']['code'] == 'UNKNOWN_TOOL'
    asyncio.run(check())


def test_personal_tool_requires_login_and_does_not_send_request():
    async def check():
        def unexpected(_request):
            pytest.fail('guest reached private endpoint')
        async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as client:
            result = await registry.execute('order', 'my_vouchers', '{}', ToolContext(java=JavaAPI(client)))
            assert result['error']['code'] == 'LOGIN_REQUIRED'
    asyncio.run(check())


@pytest.mark.parametrize('mode,expected', [('missing', 'MODEL_NOT_CONFIGURED'), ('denied', 'MODEL_HTTP_ERROR'), ('loop', 'STEP_LIMIT')])
def test_failures_are_explicit_and_do_not_save_partial_turns(mode, expected):
    async def check():
        def llm(_request):
            if mode == 'denied':
                return httpx.Response(401, json={'error': {'message': 'secret-provider-message'}})
            return completion(name='delegate_shop', arguments={'task': '查询店铺'})
        memory = TestMemory()
        app = create_app(model_transport=httpx.MockTransport(llm), memory=memory,
                         settings=Settings(orchestration_mode="legacy", api_key='' if mode == 'missing' else 'key', model='test', max_rounds=1))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                result = await client.post('/agent/chat', json={'message': '查店'})
                assert result.json()['detail']['code'] == expected, result.text
                assert not memory.rows
                assert 'secret-provider-message' not in result.text
    asyncio.run(check())


def test_stream_and_followup_retain_history_with_complete_tool_protocol():
    async def check():
        calls = []
        def llm(request):
            calls.append(json.loads(request.content))
            return completion('你好，我可以查询店铺和优惠券。')
        app = create_app(model_transport=httpx.MockTransport(llm), memory=TestMemory(), settings=Settings(orchestration_mode="legacy", api_key='key', model='test'))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                result = await client.post('/agent/chat/stream', json={'message': '你好'})
                frames = [json.loads(line[6:]) for line in result.text.splitlines() if line.startswith('data: ')]
                assert frames[0]['type'] == 'session' and frames[-1]['type'] == 'done'
                session = frames[-1]['session_id']
                result = await client.post('/agent/chat', json={'message': '你能干什么', 'session_id': session})
                assert result.status_code == 200
                assert [m['role'] for m in calls[-1]['messages']] == ['system', 'user', 'assistant', 'user']
    asyncio.run(check())


def test_real_redis_scope_lock_expiry_and_trimming():
    url = os.environ.get('AGENT_TEST_REDIS_URL')
    if not url:
        pytest.skip('set AGENT_TEST_REDIS_URL for isolated Redis integration check')
    async def check():
        from redis.asyncio import Redis
        redis = Redis.from_url(url, decode_responses=True)
        memory = RedisMemory(redis)
        scope = 'test:' + uuid.uuid4().hex
        session = None
        try:
            async with memory.turn(scope, None) as (session, history):
                with pytest.raises(AgentError, match='处理中'):
                    async with memory.turn(scope, session):
                        pass
                await memory.save(scope, session, [{'user': str(i), 'assistant': 'ok'} for i in range(10)])
            assert len(await memory.load(scope, session)) == 8
            assert await redis.ttl(memory.key(scope, session)) > 86000
            with pytest.raises(AgentError):
                await memory.load(scope + ':other', session)
            with pytest.raises(AgentError):
                async with memory.turn(scope, str(uuid.uuid4())):
                    pass
        finally:
            if session:
                await redis.delete(memory.key(scope, session), memory.key(scope, session) + ':lock')
            await redis.aclose()
    asyncio.run(check())



def test_private_wallet_forwards_only_server_token_and_normalizes_cents():
    async def check():
        def java(request):
            assert request.url.path == '/voucher-order/mine'
            assert request.headers['authorization'] == 'private-login'
            assert request.url.params['ownedOnly'] == 'true'
            return httpx.Response(200, json={'success': True, 'data': {
                'records': [{'orderId': '656670477737210969', 'payValue': 2990, 'actualValue': 5000}],
                'total': 1, 'availableCount': 1,
            }})
        async with httpx.AsyncClient(base_url='http://java.test', transport=httpx.MockTransport(java)) as client:
            result = await registry.execute('order', 'my_vouchers', '{"owned_only":true}', ToolContext(java=JavaAPI(client, 'private-login'), authenticated=True, scope='user:1'))
            assert result['records'][0]['priceYuan'] == 29.9
            assert result['records'][0]['orderId'] == '656670477737210969'
    asyncio.run(check())


def test_model_ignoring_required_tools_is_not_accepted_as_business_answer():
    async def check():
        replies = [completion(name='delegate_shop', arguments={'task': '查店铺'}), completion('我随便猜一家店')]
        def llm(_request):
            return replies.pop(0)
        app = create_app(model_transport=httpx.MockTransport(llm), memory=TestMemory(), settings=Settings(orchestration_mode="legacy", api_key='key', model='test'))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://test') as client:
                result = await client.post('/agent/chat', json={'message': '查店'})
                assert result.json()['detail']['code'] == 'MODEL_TOOLS_UNSUPPORTED'
    asyncio.run(check())


def test_voucher_results_include_business_timezone_and_normalized_price():
    async def check():
        def java(_request):
            return httpx.Response(200, json={'success': True, 'data': [{
                'id': 8, 'shopId': 4, 'type': 1, 'payValue': 1900, 'actualValue': 3000,
                'stock': 30, 'beginTime': '2020-01-01T00:00:00+00:00', 'endTime': '2099-01-01T00:00:00',
            }]})
        async with httpx.AsyncClient(base_url='http://java.test', transport=httpx.MockTransport(java)) as client:
            result = await registry.execute('voucher', 'find_vouchers', '{}', ToolContext(java=JavaAPI(client)))
            assert result['queriedAt'].endswith('+08:00')
            assert result['timezone'] == 'Asia/Shanghai'
            assert result['vouchers'][0]['activityState'] == '可抢'
            assert result['vouchers'][0]['priceYuan'] == 19
    asyncio.run(check())
