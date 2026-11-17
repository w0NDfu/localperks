import asyncio
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
from pathlib import Path

import httpx
from redis.asyncio import Redis

from model_budget import load,write
from usage import rows,export,CASE,ActualApiTransport

HERE=Path(__file__).resolve().parent


def comparable_grade(case,data,oracle):
    """Frozen common criteria for both architectures; no upgraded-only output fields."""
    failures=[];answer=data.get('answer','');trace=data.get('trace',[])
    if data.get('error') or not answer:return ['Incomplete answer or explicit error']
    tool_results=[e for e in trace if e.get('type')=='tool_result']
    successful=[e for e in tool_results if any(r.get('status')==200 for r in e.get('requests',[]))]
    if not successful:failures.append('No actual successful Java tool request')
    kind=case['kind']
    if kind=='decision':
        if not any(e.get('tool') in ('find_shops','shops_by_type','shop_details') for e in successful):failures.append('No grounded shop retrieval')
        if not any(e.get('tool') in ('find_vouchers','shop_vouchers') for e in successful):failures.append('No live voucher retrieval')
        positions=[]
        names={s['id']:s['name'] for s in oracle['shops']}
        for identifier in case['verified']:
            position=answer.find(names[identifier])
            if position<0:position=answer.find(str(identifier))
            if position<0:
                failures.append('Missing verified shop '+str(identifier));continue
            positions.append(position)
            ends=[answer.find(name,position+len(names[identifier])) for key,name in names.items() if key!=identifier]
            stop=min((p for p in ends if p>position),default=len(answer));segment=answer[position:stop]
            shop=next(s for s in oracle['shops'] if s['id']==identifier)
            voucher=next(v for v in oracle['vouchers'] if v['shopId']==identifier and v['type']==1)
            amount=max(shop['avg']*case['people']-voucher['actualValue']/100,0)+voucher['payValue']/100
            number=f'{amount:g}'
            if not re.search(r'(?:估算|预计|实付|实花|预估)[\s\S]{0,100}(?<!\d)'+re.escape(number)+r'(?:\.0+)?(?!\d)',segment):
                failures.append('Missing/incorrect explicitly estimated cash amount for '+str(identifier))
            stated_stock=re.findall(r'(?:库存|剩余|余量)[^\d]{0,8}(\d+)',segment)
            if any(int(value)!=20 for value in stated_stock):failures.append('Incorrect stated live stock for '+str(identifier))
        if positions!=sorted(positions):failures.append('Verified ranking order differs from fixed scoring oracle')
        for identifier in case['unknown']:
            position=answer.find(names[identifier]);segment=answer[position:position+650] if position>=0 else ''
            if not segment or not re.search(r'待确认|无法确认|不能保证|不保证|规则不|需要确认|需.*核实',segment):
                failures.append('Uncertain shop is missing or presented without qualification: '+str(identifier))
        assurance=re.sub(r'不能保证|不保证|无法保证','uncertain',answer)
        if re.search(r'(?:保证|肯定|一定).{0,12}(?:可用|使用)|提供.{0,8}(?:包间|停车|双人套餐)|可以叠加',assurance):
            failures.append('Unsupported eligibility/facility assurance')
    elif kind=='detail':
        if not any(e.get('tool')=='shop_details' and e.get('result',{}).get('id')==case['shop'] for e in successful):failures.append('Wrong shop evidence')
    elif kind=='types':
        if not any(e.get('tool')=='shop_types' and e.get('result') for e in successful):failures.append('Missing category evidence')
    elif kind=='vouchers':
        if not any(any(v.get('id')==93001 for v in e.get('result',{}).get('vouchers',[])) for e in successful if isinstance(e.get('result'),dict)):
            failures.append('Missing voucher93001 evidence')
    elif kind=='search':
        if not any(e.get('result',{}).get('shops') for e in successful if isinstance(e.get('result'),dict)):failures.append('Missing matching shops')
    elif kind=='order':
        if not any(e.get('tool')=='order_status' and e.get('result',{}).get('statusCode')==12 for e in successful):failures.append('Wrong owner order state')
    elif kind=='wallet':
        if not any(e.get('result',{}).get('records') for e in successful if isinstance(e.get('result'),dict)):failures.append('Missing owner wallet')
    return failures


async def comparisons(experiment,cases):
    stock_before=experiment.sql_json("SELECT JSON_OBJECT('voucher_id',voucher_id,'stock',stock) FROM tb_seckill_voucher ORDER BY voucher_id;")
    selected=[cases[i] for i in (6,7,8,9,10,11,0,14,15,20)]
    write(experiment.path/'comparison_protocol.json',{'version':1,'cases':selected,'common_grader_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
       'criteria':'Actual successful Java evidence; explicit per-shop estimated cash, fixed verified rank and uncertainty labels for complex tasks; same grader for both modes; no failures excluded',
       'parameters':{'model':experiment.env['OPENAI_MODEL'],'output_tokens':1500,'max_rounds':5,'turn_timeout':200},'fixture_hash':experiment.fixture_hash})
    results=[];gate=asyncio.Semaphore(2)
    async def one(case):
        async with gate:
            label='baseline_'+case['id'];env=dict(experiment.env,EVAL_CASE=label)
            started=time.perf_counter()
            process=await asyncio.create_subprocess_exec(sys.executable,str(HERE/'worker.py'),'baseline','--run',str(experiment.path),
                '--case',label,'--message',case['message'],stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,env=env,cwd=experiment.path.parents[2])
            try:
                out,err=await asyncio.wait_for(process.communicate(),225)
                data=json.loads(out.decode('utf-8-sig').strip().splitlines()[-1])
            except Exception as exc:
                if process.returncode is None:
                    experiment.command(['taskkill','/PID',str(process.pid),'/T','/F'],check=False)
                    await process.wait()
                data={'error':type(exc).__name__,'latency_ms':(time.perf_counter()-started)*1000,'trace':[]}
            calls=rows(experiment.path,label);failure=comparable_grade(case,data,experiment.oracle)
            result={'id':case['id'],'mode':'original','status':'PASS' if not failure else 'FAIL','failures':failure,
                    'latency_ms':data.get('latency_ms',0),'model_requests':sum(c['kind']=='chat' for c in calls),
                    'input_tokens':sum(c['input_tokens'] or 0 for c in calls),'output_tokens':sum(c['output_tokens'] or 0 for c in calls),
                    'total_tokens':sum(c['total_tokens'] or 0 for c in calls),'tool_calls':sum(e['type']=='tool_result' for e in data.get('trace',[])),
                    'degradations':sum(e['type']=='degradation' for e in data.get('trace',[])),'fixture_hash':experiment.fixture_hash}
            write(experiment.path/('comparison-original-'+case['id']+'.json'),{'case':case,'result':result,'output':data})
            experiment.stage('original-'+case['id']+'-'+result['status'])
            return result
    # First baseline request at concurrency1, then bounded concurrency2.
    results.append(await one(selected[0]))
    results.extend(await asyncio.gather(*(one(c) for c in selected[1:])))
    for case in selected:
        current=experiment.results[case['id']];data=current['output'];failure=comparable_grade(case,data,experiment.oracle)
        source=current['result']
        results.append({'id':case['id'],'mode':'upgraded','status':'PASS' if not failure else 'FAIL','failures':failure,
             'latency_ms':source['latency_ms'],'model_requests':source['actual_model_requests'],'input_tokens':source['input_tokens'],
             'output_tokens':source['output_tokens'],'total_tokens':source['total_tokens'],'tool_calls':source['tool_calls'],
             'degradations':len(source['degradations']),'fixture_hash':experiment.fixture_hash,'reused_actual_result':source['case_label']})
    stock_after=experiment.sql_json("SELECT JSON_OBJECT('voucher_id',voucher_id,'stock',stock) FROM tb_seckill_voucher ORDER BY voucher_id;")
    write(experiment.path/'comparison_stock_proof.json',{'before':stock_before,'after':stock_after,'unchanged':stock_before==stock_after})
    assert stock_before==stock_after
    write(experiment.path/'comparison_results.json',results);export(experiment.path)


async def expanded(experiment,cases):
    await experiment.start_agent()
    from models.grounded import validate_composition
    audited=[]
    bad=[]
    for case in cases[:6]:
        item=experiment.results[case['id']];data=item['output']
        try:
            validate_composition({'paragraphs':[data.get('answer_composition','')]},data.get('answer_evidence',{}),list(data.get('answer_evidence',{})))
            audited.append({'id':case['id'],'final_guard_passed':True})
        except ValueError as exc:
            audited.append({'id':case['id'],'final_guard_passed':False,'reason':str(exc)})
            bad.append(case)
    write(experiment.path/'core_final_guard_audit.json',audited)
    if any(case['id']=='A_complete' for case in bad):
        await experiment.request(cases[0],label='A_complete_final_guard')
    if any(case['id'].startswith('B') for case in bad):
        # Replay one continuous fresh conversation after a changed guard.
        session=None
        for case in cases[1:6]:
            body={'message':case['message']}
            if session:body['session_id']=session
            if case.get('ui'):body['constraints']=case['ui']
            data,_=await experiment.request(case,body,label=case['id']+'_final_guard')
            session=data.get('session_id') or session
    experiment.stage('expanded-real-agent')
    gate=asyncio.Semaphore(2)
    async def ordinary(case):
        async with gate:return await experiment.request(case)
    await asyncio.gather(*(ordinary(c) for c in cases[6:22]))
    await comparisons(experiment,cases)
    await experiment.request(cases[22])
    first,_=await experiment.request(cases[23])
    if first.get('task_id'):
        await experiment.request(cases[23],{'message':'恢复','task_id':first['task_id']},label='fault_recover_resumed')
    restore=cases[24]
    initial_case={**restore,'stock':20}
    first,_=await experiment.request(initial_case,label='checkpoint_before_restart')
    if first.get('task_id'):
        async with Redis.from_url(experiment.redis_url,decode_responses=True) as redis:
            for i in range(1,4):await redis.set('seckill:stock:'+str(93000+i),7)
            key='agent:conversation:user:10001:'+first['session_id'];await redis.delete(key)
        experiment.sql('UPDATE tb_seckill_voucher SET stock=7 WHERE voucher_id BETWEEN 93001 AND 93003;')
        write(experiment.path/'checkpoint_mutation.json',{'deleted_exact_test_session_key':key,'stock_before':20,'stock_after':7,'restart_agent_only':True})
        await experiment.start_agent()
        await experiment.request(restore,{'message':'恢复','task_id':first['task_id']},label='checkpoint_resume')
    async with Redis.from_url(experiment.redis_url,decode_responses=True) as redis:
        for i in range(1,4):await redis.set('seckill:stock:'+str(93000+i),0)
    experiment.sql('UPDATE tb_seckill_voucher SET stock=0 WHERE voucher_id BETWEEN 93001 AND 93003;')
    await experiment.request(cases[25])
    async with Redis.from_url(experiment.redis_url,decode_responses=True) as redis:
        for i in range(1,4):await redis.set('seckill:stock:'+str(93000+i),20)
    experiment.sql('UPDATE tb_seckill_voucher SET stock=20 WHERE voucher_id BETWEEN 93001 AND 93003;')
    from rag_experiment import evaluate
    await evaluate(experiment)
    await experiment.start_agent()
    for case in cases[26:]:await experiment.request(case)
    export(experiment.path)
