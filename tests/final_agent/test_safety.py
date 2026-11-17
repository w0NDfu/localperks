import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2] / 'agent-service'))
from models.grounded import validate_composition
from rag.evidence import assess
from orchestration.dialogue import exclusive_shop_keyword


def test_explicit_exclusive_shop_name_is_not_confused_with_recommendation_count():
    assert exclusive_shop_keyword('推荐三家餐厅。只看验收餐厅3。')=='验收餐厅3'
    assert exclusive_shop_keyword('仅考虑“指定饭店”。')=='指定饭店'
    assert exclusive_shop_keyword('推荐三家餐厅') is None


def test_duplicate_keyword_json_is_normalized_without_changing_the_filter(monkeypatch):
    import asyncio
    import orchestration.dialogue as dialogue
    from orchestration.state import ConsumptionConstraints
    async def response(*args,**kwargs):
        return {'keyword':'指定饭店','updates':{'keyword':'指定饭店'}}
    async def emit(event):pass
    monkeypatch.setattr(dialogue,'json_completion',response)
    parsed,_=asyncio.run(dialogue.interpret(None,'只看指定饭店。',None,ConsumptionConstraints(),None,emit,None,'Asia/Shanghai'))
    assert parsed.keyword=='指定饭店'


def test_exclusive_filter_parse_failure_stops_before_business_tools(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    import orchestration.supervisor as supervisor
    from core.state import AgentError
    async def unavailable(*args,**kwargs):raise ValueError('No reliable parsed filter')
    async def emit(event):pass
    monkeypatch.setattr(supervisor,'interpret',unavailable)
    engine=supervisor.Supervisor(SimpleNamespace(configured=True))
    with pytest.raises(AgentError) as exc:
        asyncio.run(engine.run('推荐两人吃饭的店，只看指定饭店。',[],SimpleNamespace(business_timezone='Asia/Shanghai'),emit))
    assert exc.value.code=='CONSTRAINT_PARSE_FAILED'


@pytest.mark.parametrize('text',[
    '推荐库存999张的这家{{s9002}}和{{s9001}}',
    '可以免费停车。{{s9002}}{{s9001}}',
    '我先比较{{unknown}}，再说明{{s9001}}。',
    '我先比较{{s9001}}，再说明{{s9002}}。',
    '推荐三家商店：{{s9002}}{{s9001}}',
])
def test_model_composition_rejects_unsupported_claims_or_changed_rank(text):
    with pytest.raises(ValueError):
        validate_composition({'paragraphs':[text]},{'s9002':'真实事实','s9001':'真实事实'},['s9002','s9001'])


def test_model_prose_can_select_real_fact_slots_without_rewriting_facts():
    raw={'paragraphs':['我会先比较这一项：{{s9002}}。再结合这一项作判断：{{s9001}}。']}
    text,ids=validate_composition(raw,{'s9002':'金额100元','s9001':'金额120元'},['s9002','s9001'])
    assert text==raw['paragraphs'][0] and ids==['s9002','s9001']


@pytest.mark.parametrize('query',[
    '验收餐厅9001今天包间订金具体多少元？','优惠券93001现在还剩多少库存？',
    '优惠券93001最多适用几个人？','2027年春节杭州限行尾号是什么？'])
def test_relevant_boundary_document_does_not_prove_missing_specific_fact(query):
    hits=[{'id':'a','text':'当前店铺API没有包间和预约字段。库存需要重新查询Java。'}]
    assert assess(query,hits)['sufficient'] is False


def test_static_payment_boundary_has_source_support():
    result=assess('支付说明：项目有真实支付通道吗？',[{'id':'a','text':'当前项目只有模拟支付，没有真实支付通道。'}])
    assert result['sufficient'] and result['claims'][0]['source_ids']==['a']
