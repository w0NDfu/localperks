import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

from core.config import Settings
from main import create_app
from tests_support import TestMemory


def shop(identifier=1, **changes):
    return {**dict(id=identifier, name=f'隔离餐厅{identifier}', x=120.1601, y=30.2501,
                   avgPrice=80, openHours='10:00-22:00'), **changes}


def voucher(identifier=8, **changes):
    base = dict(id=identifier, shopId=1, type=1, stock=10, title='隔离测试券',
                payValue=6000, actualValue=10000, beginTime='2020-01-01T00:00:00',
                endTime='2099-01-01T00:00:00', rules='长期有效；全天可用；每桌限1张；2人可用')
    return {**base, **changes}


def constraints(**changes):
    visit = (datetime.now(ZoneInfo('Asia/Shanghai')) + timedelta(days=1)).replace(hour=19, minute=0, second=0, microsecond=0)
    return dict(location={'longitude': 120.16, 'latitude': 30.25}, people=2,
                budget_per_person=100, visit_time=visit.isoformat(), **changes)


class BusinessFixture:
    def __init__(self):
        self.requests = []
        self.shops = [shop(i) for i in range(1, 4)]
        self.offers = {i: [voucher(i + 7, shopId=i)] for i in range(1, 4)}
        self.failed_shop = None
        self.writes = 0
        self.ambiguous = False

    async def __call__(self, request):
        import asyncio
        self.requests.append({'method': request.method, 'path': request.url.path})
        path = request.url.path
        if request.method == 'POST':
            self.writes += 1
            await asyncio.sleep(.01)
            if self.ambiguous:
                raise httpx.ReadTimeout('injected lost reply', request=request)
            data = {'orderId': '123', 'statusCode': 10}
        elif path == '/user/me':
            if request.headers.get('authorization') == 'expired':
                return httpx.Response(401)
            data = {'id': 2 if request.headers.get('authorization') == 'two' else 1}
        elif path == '/shop-type/list':
            data = [{'id': 1, 'name': 'Food'}]
        elif path in ('/shop/of/type', '/shop/of/name'):
            data = self.shops if int(request.url.params.get('current', '1')) == 1 else []
        elif path.startswith('/shop/'):
            data = self.shops[0]
        elif path.startswith('/voucher/list/'):
            identifier = int(path.rsplit('/', 1)[1])
            if identifier == self.failed_shop:
                return httpx.Response(500)
            await asyncio.sleep(.005)
            data = self.offers.get(identifier, [])
        elif path == '/voucher/market':
            data = sum(self.offers.values(), [])
        elif path == '/voucher-order/mine':
            data = {'records': [{'orderId': '123', 'voucherId': 8, 'statusCode': 10}], 'total': 1}
        elif path.startswith('/voucher-order/status/'):
            if request.headers.get('authorization') == 'two':
                return httpx.Response(200, json={'success': False})
            data = {'orderId': '123', 'statusCode': 12}
        else:
            raise AssertionError(f'Unspecified fixture endpoint: {path}')
        return httpx.Response(200, json={'success': True, 'data': data})


def completion(content=None, name=None, arguments=None):
    message = {'role': 'assistant', 'content': content}
    if name:
        message['tool_calls'] = [{'id': 'call_fixture', 'type': 'function',
                                 'function': {'name': name, 'arguments': json.dumps(arguments)}}]
    return httpx.Response(200, json={'id': 'fixture', 'object': 'chat.completion', 'created': 1, 'model': 'mock',
        'choices': [{'index': 0, 'message': message, 'finish_reason': 'tool_calls' if name else 'stop'}],
        'usage': {'prompt_tokens': 10, 'completion_tokens': 5, 'total_tokens': 15}})


def app_for(path, business=None, **settings):
    return create_app(settings=Settings(api_key='', state_dsn=os.environ['AGENT_TEST_STATE_DSN'], **settings), memory=TestMemory(),
                      transport=httpx.MockTransport(business or BusinessFixture()))
