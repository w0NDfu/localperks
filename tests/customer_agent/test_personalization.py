import asyncio
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import httpx
import pytest
from fastapi.testclient import TestClient

from core.config import Settings
from core.context import ToolContext
from decision.ranking import DecisionEngine
from main import create_app
from memory.profiles import ANSWER_STYLE, answer_style
from models.grounded import compose
from orchestration.state import ConsumptionConstraints
from orchestration.supervisor import Supervisor
from test_async_api import java
from test_platform_p0 import constraints,shop,voucher
from tests_support import TestMemory


def test_preferences_are_owned_revision_checked_and_deletable():
    app=create_app(settings=Settings(api_key='',state_dsn=os.environ['AGENT_TEST_STATE_DSN']),transport=httpx.MockTransport(java),memory=TestMemory())
    with TestClient(app) as c:
        one={'authorization':'user-one'};two={'authorization':'user-two'}
        assert c.get('/agent/preferences').status_code==401
        initial=c.get('/agent/preferences',headers=one).json()
        payload={'revision':initial['revision'],'preferences':{'budget_per_person':80,'cuisine_preferences':['粤菜'],'excluded_categories':['火锅'],'answer_style':'concise'}}
        saved=c.put('/agent/preferences',headers=one,json=payload)
        assert saved.status_code==200 and saved.json()['revision']==1
        assert c.get('/agent/preferences',headers=two).json()['preferences']=={}
        assert c.put('/agent/preferences',headers=one,json=payload).status_code==409
        assert c.put('/agent/preferences',headers=one,json={**payload,'scope':'user:2'}).status_code==422
        inherited=c.post('/agent/chat',headers=one,json={'message':'推荐附近餐厅'}).json()
        assert inherited['constraints']['budget_per_person']==80
        overridden=c.post('/agent/chat',headers=one,json={'message':'推荐附近餐厅，人均100元'}).json()
        assert overridden['constraints']['budget_per_person']==100
        cleared=c.delete('/agent/preferences?revision=1',headers=one).json()
        assert cleared['revision']==2 and cleared['preferences'].get('budget_per_person') is None
        assert not cleared['preferences'].get('cuisine_preferences')
        assert c.put('/agent/preferences',headers=one,json=payload).status_code==409
        fresh=c.post('/agent/chat',headers=one,json={'message':'推荐附近餐厅'}).json()
        assert fresh['constraints']['budget_per_person'] is None


def test_current_request_overrides_session_and_session_overrides_profile(monkeypatch):
    async def run():
        ctx=ToolContext(java=None,preferences={'budget_per_person':60,'radius_m':1000,'answer_style':'detailed','cuisine_preferences':['日料']})
        supervisor=Supervisor(Settings(api_key=''))
        captured={}
        async def recommend(value,*args,**kwargs):
            captured.update(value.model_dump());return {'constraints':value.model_dump()}
        monkeypatch.setattr(supervisor,'recommend',recommend)
        history=[{'constraints':constraints(budget_per_person=90,radius_m=2000,visit_time='2090-11-05T19:00:00+08:00').model_dump()}]
        async def emit(event):pass
        await supervisor.run('推荐附近餐厅，人均80元，简洁回答',history,ctx,emit)
        assert captured['budget_per_person']==80 and captured['people']==2 and captured['radius_m']==2000
        assert captured['answer_style']=='concise'
        assert ctx.preferences['budget_per_person']==60
    asyncio.run(run())


def test_preferences_filter_and_rank_real_candidates_without_overriding_budget():
    opts=constraints(budget_per_person=100)
    opts.cuisine_preferences=['日料'];opts.excluded_categories=['火锅']
    shops=[shop(1,category_name='川菜'),shop(2,category_name='日料'),shop(3,category_name='火锅'),shop(4,category_name='日料',avgPrice=500),shop(5,category_name='美食')]
    offers={s['id']:[voucher(id=s['id'],shopId=s['id'])] for s in shops}
    result=DecisionEngine().rank(shops,offers,opts,now=datetime(2026,11,5,16,tzinfo=ZoneInfo('Asia/Shanghai')))
    assert result['recommendations'][0]['shop_id']==2
    assert {r['shop_id'] for r in result['rejected']}=={3,4}
    assert {r['shop_id'] for r in result['unverified_candidates']}=={5}


def test_answer_style_context_is_isolated_between_concurrent_users():
    async def run():
        prompts=[]
        async def wire(request):
            body=json.loads(request.content);prompts.append(body['messages'][0]['content'])
            await asyncio.sleep(.01)
            return httpx.Response(200,json={'id':'fake','object':'chat.completion','created':0,'model':'fake',
                'choices':[{'index':0,'message':{'role':'assistant','content':json.dumps({'paragraphs':['根据已经核对的资料，{{s1}}']})},'finish_reason':'stop'}]})
        cfg=Settings(api_key='test',model='fake')
        async def emit(event):pass
        async def user(style):
            with answer_style(style):
                await compose(cfg,'推荐',{'s1':'餐厅事实'},emit,transport=httpx.MockTransport(wire))
        await asyncio.gather(user('concise'),user('detailed'))
        assert sum('最多两个段落' in p for p in prompts)==1
        assert sum('详细的中文解释' in p for p in prompts)==1
        assert ANSWER_STYLE.get()=='neutral'
    asyncio.run(run())


def test_model_cannot_replace_an_explicit_budget_with_a_different_value(monkeypatch):
    async def run():
        from orchestration import dialogue
        from orchestration.planner import Planner
        async def wrong(*args,**kwargs):
            return {'keyword':'','explanation':'','updates':{'budget_per_person':1000}}
        monkeypatch.setattr(dialogue,'json_completion',wrong)
        async def emit(event):pass
        parsed=Planner().constraints('人均80元')
        with pytest.raises(ValueError,match='explicitly parsed'):
            await dialogue.interpret(Settings(api_key=''),'人均80元',None,parsed,{},emit,None,'Asia/Shanghai')
        assert parsed.budget_per_person==80
    asyncio.run(run())
