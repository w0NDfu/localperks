"""A bounded post-fix real recheck; keeps the original case and independent oracle."""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parent))
from repair_live import Experiment,SimpleNamespace,httpx,load,write,rows,export


async def main(run):
    run=Path(run).resolve()
    deadline=time.monotonic()+360
    while not (run/'task-rag_payment.json').exists():
        if time.monotonic()>deadline:raise TimeoutError('No restarted final Agent available')
        await asyncio.sleep(2)
    case=next(c for c in load(run/'agent_dataset.json')['cases'] if c['id']=='c_unknown_rule')
    label='c_unknown_rule_exclusive_filter_fix';request={'message':case['message']}
    start=time.perf_counter()
    async with httpx.AsyncClient(timeout=220,trust_env=False) as client:
        response=await client.post(load(run/'isolation.json')['agent_url']+'/agent/chat',json=request,
            headers={'authorization':run.name+'-user-1','X-Eval-Case':label})
    data=response.json();data.update(http_status=response.status_code,latency_ms=(time.perf_counter()-start)*1000)
    failures=Experiment.grade(SimpleNamespace(oracle=load(run/'fixture_oracle.json')),case,data)
    calls=rows(run,label)
    result={'id':case['id'],'case_label':label,'mode':'upgraded','status':'PASS' if not failures else 'FAIL','failures':failures,
        'latency_ms':data['latency_ms'],'actual_model_requests':sum(c['kind']=='chat' for c in calls),
        'input_tokens':sum(c['input_tokens'] or 0 for c in calls),'output_tokens':sum(c['output_tokens'] or 0 for c in calls),
        'total_tokens':sum(c['total_tokens'] or 0 for c in calls),'usage_missing':sum(c['total_tokens'] is None for c in calls),
        'tool_calls':sum(e['type']=='tool_result' for e in data.get('trace',[])),
        'degradations':[e for e in data.get('trace',[]) if e['type']=='degradation']}
    write(run/('task-'+label+'.json'),{'case':case,'request':request,'result':result,'output':data})
    supplemental=load(run/'supplemental_results.json',{});supplemental[case['id']]={'case':case,'result':result,'output':data}
    write(run/'supplemental_results.json',supplemental);export(run)
    print(result,flush=True)


if __name__=='__main__':asyncio.run(main(sys.argv[1]))
