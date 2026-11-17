import argparse
import asyncio
import hashlib
import importlib
import json
import os
import secrets
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime,timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
from redis.asyncio import Redis

HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[1];AGENT=ROOT / 'agent-service'
sys.path[:0]=[str(HERE),str(ROOT/'tests/real_e2e'),str(AGENT)]
from run import Run
from model_budget import load,write
from usage import export,rows,connection


class Experiment(Run):
    def __init__(self,path):
        self.path=Path(path).resolve();self.id=self.path.name
        assert self.path.parent==(ROOT/'artifacts/test-runs').resolve()
        self.manifest=load(self.path/'resource_manifest.json');assert self.manifest['run_id']==self.id
        self.project=self.manifest['compose_project'];self.containers={};self.processes=[];self.files=[]
        self.stop_monitor=threading.Event();self.sample_lock=threading.Lock();self.failure=None
        self.mysql_password=secrets.token_hex(20);self.java_rows=[];self.agent_rows=[];self.rag_rows=[]
        self.env=dict(os.environ,TEMP=str(self.path/'temp'),TMP=str(self.path/'temp'),PYTHONDONTWRITEBYTECODE='1',PYTHONIOENCODING='utf-8')
        self.results={};self.oracle={};self.server=None
        self.tomorrow=(datetime.now(ZoneInfo('Asia/Shanghai'))+timedelta(days=1)).date().isoformat()
        self.ui={'location':{'longitude':120.16,'latitude':30.25},'budget_per_person':100,'visit_time':self.tomorrow+'T19:00:00+08:00','radius_m':1000}
        connection(self.path).close()

    def build(self):
        self.jar=ROOT/'target/localperks-backend-0.0.1-SNAPSHOT.jar'
        assert self.jar.exists(),'Reuse previously validated JAR; build only if sources changed'
        changed=[p for folder in ('src/main/java','src/main/resources') for p in (ROOT/folder).rglob('*') if p.is_file() and p.stat().st_mtime>self.jar.stat().st_mtime]
        if changed:super().build()
        else:
            write(self.path/'versions.json',{'git_head':self.command(['git','rev-parse','HEAD']).stdout.strip(),
              'jar_sha256':hashlib.sha256(self.jar.read_bytes()).hexdigest(),'jar_reused':True,
              'baseline_sha256':hashlib.sha256((AGENT/'evaluations/baseline_v02.zip').read_bytes()).hexdigest(),
              'normal_volumes_before':self.command(['docker','volume','ls','--format','{{.Name}}']).stdout.splitlines(),
              'historical_45955_ledger_sha256':hashlib.sha256((ROOT/'artifacts/test-runs/v03-20261105-202629-0258a123/api_usage.json').read_bytes()).hexdigest()})

    async def fixtures(self):
        self.sql("UPDATE tb_shop SET avg_price=75,x=120.162,y=30.252 WHERE id=9002; UPDATE tb_shop SET open_hours='10:00-23:59';")
        async with Redis.from_url(self.redis_url,decode_responses=True) as redis:
            await redis.geoadd('shop:geo:1',[120.162,30.252,'9002'])
        for i in range(1,4):
            await self.coupon(93000+i,20,shop=9000+i,pay=[6000,5000,3000][i-1],
                rules='以店铺说明为准' if i==3 else '长期有效；全天可用；每桌限1张')
        self.sql("INSERT INTO tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) VALUES(94001,9001,'隔离账号已购券','测试订单fixture',1900,3000,0,1);")
        self.order_id='700000000000000001';self.paid_id='700000000000000002'
        self.sql(f"INSERT INTO tb_voucher_order(id,user_id,voucher_id,status) VALUES({self.order_id},10001,94001,1),({self.paid_id},10002,94001,2);")
        self.oracle={'shops':self.sql_json("SELECT JSON_OBJECT('id',id,'name',name,'avg',avg_price,'x',x,'y',y,'openHours',open_hours) FROM tb_shop ORDER BY id;"),
           'vouchers':self.sql_json("SELECT JSON_OBJECT('id',id,'shopId',shop_id,'payValue',pay_value,'actualValue',actual_value,'rules',rules,'type',type) FROM tb_voucher ORDER BY id;"),
           'orders':self.sql_json("SELECT JSON_OBJECT('id',id,'user',user_id,'voucher',voucher_id,'status',status) FROM tb_voucher_order ORDER BY id;")}
        write(self.path/'fixture_oracle.json',self.oracle)
        self.fixture_hash=hashlib.sha256(json.dumps(self.oracle,sort_keys=True).encode()).hexdigest()
        self.env.update(EVAL_REAL_JAVA=self.java_url,SHOPLINK_BASE_URL=self.agent_url+'/evaluation-java',
                        EVAL_LOGIN_TOKEN=self.tokens[10001])
        from core.config import load_settings
        cfg=load_settings();self.env.update(OPENAI_API_KEY=cfg.api_key,OPENAI_MODEL=cfg.model,OPENAI_BASE_URL=cfg.base_url)
        await self.start_agent()

    async def start_agent(self):
        if self.server and self.server.poll() is None:
            self.command(['taskkill','/PID',str(self.server.pid),'/T','/F'],check=False);self.server.wait(timeout=10)
        stream=(self.path/'agent-server.log').open('a',encoding='utf-8');self.files.append(stream)
        self.server=subprocess.Popen([sys.executable,str(HERE/'worker.py'),'server','--run',str(self.path),'--port',str(self.agent_port)],
             cwd=ROOT,env=self.env,stdout=stream,stderr=subprocess.STDOUT);self.processes.append(self.server)
        async with httpx.AsyncClient(timeout=2,trust_env=False) as client:
            for _ in range(35):
                try:
                    if (await client.get(self.agent_url+'/health')).status_code==200:return
                except Exception:pass
                if self.server.poll() is not None:raise RuntimeError('Agent server stopped; inspect log')
                await asyncio.sleep(1)
        raise RuntimeError('Agent server startup timeout')

    def dataset(self):
        c=lambda text,people=2,budget=100,radius=1000,verified=(9002,9001),unknown=(9003,):{
            'message':text,'kind':'decision','people':people,'budget':budget,'radius':radius,'verified':list(verified),'unknown':list(unknown),'llm':True}
        base=f'帮我推荐附近1公里内适合两个人吃饭、人均100元以内、{self.tomorrow} 19:00有优惠的三家餐厅。位置120.16,30.25。逐店给出估算实付和规则依据，规则不清的不能保证可用。'
        cases=[{'id':'A_complete',**c(base)},
          {'id':'B1','message':'帮我找附近适合两个人吃饭且有优惠的店','kind':'decision','people':2,'budget':100,'radius':1000,'verified':[9002,9001],'unknown':[9003],'llm':True,'ui':self.ui},
          {'id':'B2','message':'人均预算改成80元','kind':'decision','people':2,'budget':80,'radius':1000,'verified':[9002,9001],'unknown':[9003],'llm':True},
          {'id':'B3','message':'改成三个人','kind':'decision','people':3,'budget':80,'radius':1000,'verified':[9002,9001],'unknown':[9003],'llm':True},
          {'id':'B4','message':'第二家有什么优惠？','kind':'followup','target':9001,'llm':True},
          {'id':'B5','message':'今晚确定能使用吗？','kind':'followup','target':9001,'uncertainty':True,'llm':True},
          {'id':'q_detail','message':'查询店铺9002详情','kind':'detail','shop':9002,'llm':False},
          {'id':'q_types','message':'查询店铺分类列表','kind':'types','llm':False},
          {'id':'q_vouchers','message':'查询店铺9001优惠券','kind':'vouchers','shop':9001,'llm':False},
          {'id':'q_name','message':'查询验收餐厅店铺','kind':'search','llm':False},
          {'id':'q_order','message':f'订单{self.order_id}状态','kind':'order','status':12,'llm':False},
          {'id':'q_wallet','message':'我的券包','kind':'wallet','llm':False},
          {'id':'q_paid','message':f'订单{self.paid_id}状态','kind':'denied','llm':False},
          {'id':'q_missing_shop','message':'查询店铺99999详情','kind':'not_found','llm':False},
          {'id':'c_budget',**c(base.replace('人均100元','人均80元'),budget=80)},
          {'id':'c_radius',**c(base.replace('1公里','0.1公里'),radius=100,verified=(9001,))},
          {'id':'c_people',**c(base.replace('两个人','四个人'),people=4)},
          {'id':'c_closed',**c(base.replace('19:00','02:00'),verified=(),unknown=())},
          {'id':'c_no_shops',**c(base.replace('120.16,30.25','121.16,31.25'),verified=(),unknown=())},
          {'id':'c_unknown_rule',**c(base+'只看验收餐厅3。',verified=())},
          {'id':'c_natural',**c(f'请给三位推荐有优惠的餐厅，每位预算八十元，限一公里范围，{self.tomorrow} 19:00用餐，坐标120.16,30.25。要计算一张券后的估算实付。',people=3,budget=80)},
          {'id':'c_low_budget',**c(base.replace('人均100元','人均70元'),budget=70,verified=())},
          {'id':'fault_partial',**c(base,unknown=()),'fault':True,'partial':True},
          {'id':'fault_recover',**c(base),'fault':True,'recover':True},
          {'id':'checkpoint_resume',**c(base),'restore':True,'stock':7},
          {'id':'stock_zero',**c(base,verified=(),unknown=()),'stock':0},
          {'id':'rag_payment','message':'支付说明：项目会真实扣款吗？','kind':'rag','abstain':False,'llm':True},
          {'id':'rag_validity','message':'请解释使用规则：购买活动的结束时间就是核销有效期吗？','kind':'rag','abstain':False,'llm':True},
          {'id':'rag_privacy','message':'解释登录规则：知道别人订单编号就能查询吗？','kind':'rag','abstain':False,'llm':True},
          {'id':'rag_deposit','message':'按知识库规则，验收餐厅9001今天包间订金具体多少元？','kind':'rag','abstain':True,'llm':True}]
        assert len(cases)==30
        write(self.path/'agent_dataset.json',{'fixture_hash':self.fixture_hash,'cases':cases})
        return cases

    def grade(self,case,data):
        errors=[];kind=case['kind'];trace=data.get('trace',[]);text=data.get('answer','')
        if case.get('llm'):
            if data.get('answer_origin')!='llm_grounded':errors.append('Final answer is not model-authored grounded composition')
            if any(e['type']=='degradation' for e in trace):errors.append('Degradation occurred')
            facts=data.get('answer_evidence',{});composition=data.get('answer_composition','')
            for key in __import__('re').findall(r'\{\{(\w+)\}\}',composition):composition=composition.replace('{{'+key+'}}',str(facts.get(key,'UNKNOWN')))
            if composition!=text:errors.append('Model composition does not reproduce final answer')
        if kind=='decision':
            opts=data.get('constraints',{});decision=data.get('decision',{})
            for key,expected in [('people',case['people']),('budget_per_person',case['budget']),('radius_m',case['radius'])]:
                if opts.get(key)!=expected:errors.append(f'{key}: expected {expected}, got {opts.get(key)}')
            verified=decision.get('recommendations',[]);unknown=decision.get('unverified_candidates',[])
            if [r['shop_id'] for r in verified]!=case['verified']:errors.append('Wrong verified ranking')
            if [r['shop_id'] for r in unknown]!=case['unknown']:errors.append('Wrong uncertain candidates')
            for row in verified+unknown:
                shop=next(s for s in self.oracle['shops'] if s['id']==row['shop_id'])
                voucher=next(v for v in self.oracle['vouchers'] if v['id']==row['voucher_id'])
                expected=max(shop['avg']*case['people']-voucher['actualValue']/100,0)+voucher['payValue']/100
                if row['estimated_cash_cost']!=expected:errors.append('Amount differs from independent SQL oracle')
                if row['stock']!=case.get('stock',20):errors.append('Live stock was not refreshed')
            if case.get('partial') and not decision.get('warnings'):errors.append('Partial tool failure was hidden')
        elif kind=='followup':
            if data.get('data',{}).get('shop_id')!=case['target']:errors.append('Wrong ordinal/focus reference')
            if case.get('uncertainty') and '无法' not in text and '不能' not in text:errors.append('Uncertainty not stated')
        elif kind=='detail' and data.get('data',{}).get('id')!=case['shop']:errors.append('Wrong shop')
        elif kind=='types' and not data.get('data'):errors.append('Empty categories')
        elif kind=='vouchers' and not data.get('data',{}).get('vouchers'):errors.append('Missing actual vouchers')
        elif kind=='search' and not data.get('data',{}).get('shops'):errors.append('Missing shop search')
        elif kind=='order' and data.get('data',{}).get('statusCode')!=case['status']:errors.append('Wrong owner order status')
        elif kind=='wallet' and not data.get('data',{}).get('records'):errors.append('Empty owner wallet')
        elif kind in ('denied','not_found') and data.get('state')!='failed':errors.append('Unauthorized/nonexistent resource not rejected')
        elif kind=='rag' and data.get('abstained')!=case['abstain']:errors.append('Wrong evidence sufficiency/abstention')
        if not case.get('llm') and any(e['type']=='model_request' for e in trace):errors.append('Fast route unnecessarily invoked model')
        return errors

    async def request(self,case,body=None,label=None):
        label=label or case['id'];payload=body or {'message':case['message'],**({'constraints':case['ui']} if case.get('ui') else {})}
        start=time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=225,trust_env=False) as client:
                response=await client.post(self.agent_url+'/agent/chat',json=payload,headers={'authorization':self.tokens[10001],'X-Eval-Case':label})
            try:data=response.json()
            except ValueError:data={'error':'NON_JSON'}
            data['http_status']=response.status_code
        except Exception as exc:data={'http_status':0,'error':type(exc).__name__}
        data['latency_ms']=(time.perf_counter()-start)*1000
        errors=self.grade(case,data)
        calls=rows(self.path,label)
        result={'id':case['id'],'case_label':label,'mode':'upgraded','status':'PASS' if not errors else 'FAIL','failures':errors,
             'latency_ms':data['latency_ms'],'actual_model_requests':sum(c['kind']=='chat' for c in calls),
             'input_tokens':sum(c['input_tokens'] or 0 for c in calls),'output_tokens':sum(c['output_tokens'] or 0 for c in calls),
             'total_tokens':sum(c['total_tokens'] or 0 for c in calls),'usage_missing':sum(c['total_tokens'] is None for c in calls),
             'tool_calls':sum(e['type']=='tool_result' for e in data.get('trace',[])),
             'degradations':[e for e in data.get('trace',[]) if e['type']=='degradation'],'fixture_hash':self.fixture_hash}
        write(self.path/('task-'+label+'.json'),{'case':case,'request':payload,'result':result,'output':data})
        self.results[case['id']]={'case':case,'result':result,'output':data}
        export(self.path);self.stage('task-'+label+'-'+result['status'])
        return data,result

    async def core(self,cases,attempt=1):
        _,first=await self.request(cases[0],label='A_complete' if attempt==1 else f'A_complete_retry{attempt}')
        history=None;outcomes=[first]
        for case in cases[1:6]:
            payload={'message':case['message']}
            if case.get('ui'):payload['constraints']=case['ui']
            if history:payload['session_id']=history
            data,result=await self.request(case,payload,label=case['id'] if attempt==1 else case['id']+'_retry'+str(attempt))
            history=data.get('session_id') or history;outcomes.append(result)
        return all(r['status']=='PASS' for r in outcomes)

    def backup_tasks(self):
        source=Path(self.env['AGENT_TASK_DB'])
        if source.exists():
            from contextlib import closing
            with closing(sqlite3.connect(source)) as src,closing(sqlite3.connect(self.path/'tasks-evidence.sqlite3')) as dst:
                src.backup(dst)


async def main(args):
    experiment=Experiment(args.run)
    try:
        experiment.sample('before');experiment.build();await experiment.infrastructure()
        experiment.monitor_thread=threading.Thread(target=experiment.monitor,daemon=True);experiment.monitor_thread.start()
        await experiment.fixtures();cases=experiment.dataset()
        for attempt in range(1,4):
            success=await experiment.core(cases,attempt)
            write(experiment.path/'core_gate.json',{'attempt':attempt,'passed':success})
            if success:break
            experiment.stage('CORE_NEEDS_FIX')
            sentinel=experiment.path/'core_control.json'
            for _ in range(180):
                if sentinel.exists():sentinel.unlink();break
                await asyncio.sleep(10)
            else:raise RuntimeError('Core correction wait expired')
            await experiment.start_agent()
        from phases import expanded
        await expanded(experiment,cases)
        experiment.sample('end')
    except BaseException as exc:
        experiment.failure=type(exc).__name__+': '+str(exc)
        write(experiment.path/'experiment_failure.json',{'error':experiment.failure})
        print(experiment.failure,flush=True)
    finally:
        experiment.backup_tasks()
        export(experiment.path)
        try:experiment.cleanup()
        except Exception as exc:write(experiment.path/'cleanup_failure.json',{'error':str(exc)})
        write(experiment.path/'experiment_state.json',{'failure':experiment.failure,'results':experiment.results,'oracle':experiment.oracle})
        try:
            from final_report import reports
            reports(experiment.path)
        except Exception as exc:print('Report pending: '+str(exc),flush=True)
    print('FINAL_RUN '+str(experiment.path),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--run',required=True);asyncio.run(main(p.parse_args()))
