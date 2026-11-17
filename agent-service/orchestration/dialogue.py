import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from core.state import AgentError
from models.grounded import compose
from models.structured import json_completion
from orchestration.state import ConsumptionConstraints


class ParsedRequest(BaseModel):
    model_config=ConfigDict(extra='forbid')
    keyword: str=Field(default='',max_length=128)
    explanation: str=Field(default='',max_length=800)
    updates: dict=Field(default_factory=dict)


def exclusive_shop_keyword(message):
    """An explicit exclusive name must survive model interpretation."""
    match=re.search(r'(?:只看|仅看|只考虑|仅考虑)\s*([^，。！？；\n]{1,128})',message)
    return match[1].strip().strip('“”"') if match else None


async def interpret(settings,message,previous,parsed,supplied,emit,transport,timezone):
    correction=None
    for attempt in range(2):
        try:
            return await _interpret_attempt(settings,message,previous,parsed,supplied,emit,transport,timezone,correction)
        except ValueError as exc:
            await emit({'type':'interpretation_rejected','reason':str(exc),'attempt':attempt+1})
            if attempt: raise
            correction='上次结构化解析未通过校验：'+str(exc)+'。只输出keyword、explanation、updates；updates只含本轮明确变化，未变化字段可以省略。店名keyword必须是用户原文中的明确名称，否则空字符串。'


async def _interpret_attempt(settings,message,previous,parsed,supplied,emit,transport,timezone,correction=None):
    messages=[
        {'role':'system','content':'解析消费需求，只输出JSON：keyword、explanation、updates。updates只含用户本轮明确新增或修改的people、budget_per_person、radius_m、visit_time、location、top_k。数值规范化，visit_time用带时区ISO日期。今晚但没有钟点时visit_time=null，不猜时间或位置。只保留用户明确的店名keyword，否则空。不得把菜系/预算当店名。不要改动未提及的条件。'},
        {'role':'user','content':json.dumps({'message':message,'previous':previous,'supplied':supplied,
            'deterministic_parse':parsed.model_dump(),'explicit_exclusive_name':exclusive_shop_keyword(message),
            'now':datetime.now(ZoneInfo(timezone)).isoformat()},ensure_ascii=False)}]
    if correction: messages.append({'role':'user','content':correction})
    raw=await json_completion(settings,messages,emit,transport=transport,purpose='interpretation')
    await emit({'type':'interpretation_candidate','value':raw})
    value=ParsedRequest.model_validate(raw)
    # Some JSON models echo the top-level keyword in updates. Normalize only an
    # exact duplicate already validated against user text, never a new filter.
    if value.updates.get('keyword')==value.keyword and value.keyword and value.keyword in message:
        value.updates.pop('keyword')
    exclusive=exclusive_shop_keyword(message)
    if exclusive and value.keyword!=exclusive:
        raise ValueError('Explicit exclusive shop filter must be preserved as keyword: '+exclusive)
    allowed={'people','budget_per_person','radius_m','visit_time','location','top_k'}
    if set(value.updates)-allowed:raise ValueError('Unsupported constraint field')
    evidence={'people':r'人|位','budget_per_person':r'预算|人均|每人|每位|改成|改为',
              'radius_m':r'公里|米|范围','visit_time':r'今晚|明晚|今天|明天|时间|用餐|\d{4}-\d{2}-\d{2}|\d[:：]\d|点',
              'location':r'-?\d+\.\d+\s*[,，]\s*-?\d+\.\d+', 'top_k':r'推荐|家'}
    data=parsed.model_dump()
    from orchestration.planner import Planner
    explicit=Planner().constraints(message,timezone=timezone).model_dump(exclude_unset=True)
    for key,item in value.updates.items():
        if key in explicit and item!=explicit[key]:
            raise ValueError('Model update conflicts with an explicitly parsed user constraint: '+key)
        if data.get(key)==item:continue
        if not re.search(evidence[key],message) and (key not in (supplied or {}) or item != supplied[key]):
            raise ValueError('Constraint update has no user provenance: '+key)
        data[key]=item
    if value.keyword:
        if value.keyword not in message:raise ValueError('Invented shop keyword')
        data['keyword']=value.keyword
    # Structured UI choices win; normalized model values remain schema checked.
    result=ConsumptionConstraints.model_validate(data)
    await emit({'type':'constraint_interpretation','updates':value.updates,'validated':result.model_dump()})
    return result,value.model_dump()


def decision_context(decision):
    rows=decision['recommendations']+decision['unverified_candidates']
    return {'displayed_shop_ids':list(dict.fromkeys(r['shop_id'] for r in rows)),
            'names':{str(r['shop_id']):r['shop_name'] for r in rows},'focus_shop_id':None}


def decision_facts(decision):
    c=decision['constraints']
    facts={'constraints':f"人数{c['people']}，人均预算{c['budget_per_person']}元，半径{c['radius_m']}米，用餐时间{c['visit_time']}。",
           'limitations':'；'.join(decision['limitations'])+'。座位及接待能力未经接口验证。'}
    for row in decision['recommendations']+decision['unverified_candidates']:
        status='按明确规则通过校验' if row['eligibility']=='verified' else '使用条件待确认，不能保证指定日期可用'
        facts['s'+str(row['shop_id'])]=(f"{row['shop_name']}（店铺{row['shop_id']}、券{row['voucher_id']}）：{status}；"
             f"直线距离{row['distance_m']}米，参考人均{row['average_price']}元，券价{row['voucher_price']}元、面值{row['face_value']}元，"
             f"券面差额{row['face_saving']}元，以{row['people']}人使用一张券估算实付{row['estimated_cash_cost']}元、人均{row['estimated_per_person']}元；"
             f"库存{row['stock']}张（{row['queried_at']}查询快照）。规则：{row['rules']}；依据：{row['eligibility_reason']}；来源{row['source']}。")
    if not decision['recommendations']:
        facts['no_verified']='没有足够证据确认满足全部条件的已验证推荐，不能凑数。'
    if decision['warnings']:
        facts['partial']='部分业务接口查询失败，本次仅比较成功取得证据的店铺，未把失败当作没有优惠。'
    return facts


async def followup(supervisor,message,history,context,previous,ctx,emit):
    class Reference(BaseModel):
        model_config=ConfigDict(extra='forbid')
        shop_id: int|None=None
    ids=context.get('displayed_shop_ids',[])
    raw=await json_completion(supervisor.settings,[
        {'role':'system','content':'解析对话中的店铺指代，只输出JSON {"shop_id":整数或null}。第几家按displayed_shop_ids顺序；这家/今晚指focus_shop_id，否则最后明确讨论的店铺。只能选提供的ID，无法确定则null，不猜。'},
        {'role':'user','content':json.dumps({'message':message,'context':context,'recent_user_messages':[t['user'] for t in history[-2:]]},ensure_ascii=False)}],
        emit,transport=supervisor.model_transport,purpose='reference_resolution')
    target=Reference.model_validate(raw).shop_id
    ordinal=re.search(r'第([一二两三四五六七八九十\d]+)家',message)
    if ordinal:
        word=ordinal[1];number=int(word) if word.isdigit() else {'一':1,'二':2,'两':2,'三':3,'四':4,'五':5}.get(word,0)
        expected=ids[number-1] if 0<number<=len(ids) else None
        if target!=expected:raise AgentError('REFERENCE_CONFLICT','店铺指代未通过顺序校验',502)
    if target not in ids:
        authored=await compose(supervisor.settings,message,{'clarify':'无法确定你指的是哪家店，请提供店名或店铺ID。'},emit,transport=supervisor.model_transport,purpose='clarification_answer')
        return {**authored,'intent':'followup','state':'waiting_input','constraints':previous,'decision_context':context}
    import asyncio
    details,offers=await asyncio.gather(supervisor.executor.execute('shop','shop_details',{'shop_id':target},ctx,emit),
                                      supervisor.voucher.for_shop(target,ctx,emit))
    if details.status!='completed' or offers.status!='completed':
        raise AgentError('FOLLOWUP_QUERY_FAILED','无法取得该店的最新证据',502)
    shop=details.data;rows=offers.data['vouchers']
    normalized=[]
    for row in rows:
        scale=100 if row.get('type')==1 else 1
        normalized.append(f"券{row.get('id')}：券价{row.get('payValue',0)/scale}元，面值{row.get('actualValue',0)/scale}元，库存{row.get('stock','未知')}；购买活动{row.get('beginTime','未知')}至{row.get('endTime','未知')}，使用规则：{row.get('rules','未提供')}。")
    facts={'reference':f"店铺指代已经按会话顺序解析完成，本次明确目标是{shop.get('name')}（店铺{target}）；不是缺少目标店铺。",
           'shop':f"当前讨论{shop.get('name')}（店铺{target}），营业时间{shop.get('openHours')}，参考人均{shop.get('avgPrice')}元。来源/shop/{target}。",
           'offers':'当前优惠券实时接口证据：'+'；'.join(normalized),
           'limits':'购买活动时间不等于核销使用有效期；库存是查询快照，不能保证最终资格；不能把未提供的座位或接待能力当作已确认。'}
    updated_constraints=previous
    if re.search(r'今晚|确定|能用|能使用|可用|保证',message):
        from orchestration.planner import Planner
        proposed=Planner().constraints(message,previous,timezone=ctx.business_timezone)
        updated_constraints=proposed.model_dump()
        facts['uncertainty']='无法仅凭已有购买时间或笼统的今晚保证可用；需明确具体用餐时刻，并核对券的使用日期、时段、人数和单张规则，未知条件应联系商家确认。'
    context={**context,'focus_shop_id':target}
    authored=await compose(supervisor.settings,message,facts,emit,transport=supervisor.model_transport,purpose='followup_answer')
    await emit({'type':'reference_resolved','shop_id':target,'displayed_shop_ids':ids})
    return {**authored,'intent':'followup','state':'completed','constraints':updated_constraints,'decision_context':context,
            'data':{'shop':shop,'vouchers':rows,'shop_id':target}}
