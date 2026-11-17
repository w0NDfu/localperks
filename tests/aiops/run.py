"""P4: real fault injection only into manifest-owned empty Docker instances."""
import asyncio
import argparse
import hashlib
import json
import os
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import traceback
import zipfile
from contextlib import closing
from datetime import datetime
from pathlib import Path

import httpx
from redis.asyncio import Redis

HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[1]
sys.path[:0]=[str(ROOT/'tests/final_agent'),str(ROOT/'tests/real_e2e'),str(ROOT / 'agent-service')]
from experiment import Experiment
from model_budget import load,write
from core.config import load_settings


class P4Run(Experiment):
    def build(self):
        self.jar=ROOT/'target/localperks-backend-0.0.1-SNAPSHOT.jar'
        changed=[p for folder in ('src/main/java','src/main/resources') for p in (ROOT/folder).rglob('*') if p.is_file() and (not self.jar.exists() or p.stat().st_mtime>self.jar.stat().st_mtime)]
        if changed:
            import shutil
            result=self.command([shutil.which('mvn.cmd'),'-o','-B','-DskipTests','package'],timeout=150,check=False)
            (self.path/'maven-package.log').write_text(result.stdout+result.stderr,encoding='utf-8')
            if result.returncode:raise RuntimeError('Offline Java build failed; see maven-package.log')
        write(self.path/'versions.json',{'git_head':self.command(['git','rev-parse','HEAD']).stdout.strip(),
            'jar_sha256':hashlib.sha256(self.jar.read_bytes()).hexdigest(),'jar_reused':not changed,
            'normal_volumes_before':self.command(['docker','volume','ls','--format','{{.Name}}']).stdout.splitlines()})
    async def start_agent(self):
        if self.server and self.server.poll() is None:
            self.command(['taskkill','/PID',str(self.server.pid),'/T','/F'],check=False);self.server.wait(timeout=10)
        stream=(self.path/'agent-server.log').open('a',encoding='utf-8');self.files.append(stream)
        self.server=subprocess.Popen([sys.executable,str(HERE/'server.py'),'--port',str(self.agent_port)],
            cwd=ROOT,env=self.env,stdout=stream,stderr=subprocess.STDOUT);self.processes.append(self.server)
        async with httpx.AsyncClient(timeout=2,trust_env=False) as client:
            for _ in range(35):
                try:
                    if (await client.get(self.agent_url+'/health')).status_code==200:return
                except Exception:pass
                if self.server.poll() is not None:raise RuntimeError('P4 Agent startup failed')
                await asyncio.sleep(1)
        raise RuntimeError('P4 Agent readiness timeout')

    async def probe(self,identifier,symptom,voucher,expected,scope=None):
        end=int(time.time()*1000)
        scope=scope or {'start':end-2000,'end':end,'voucher_id':voucher,'ip':'127.0.0.1'}
        body={'symptom':symptom,'scope':scope}
        write(self.path/('truth-'+identifier+'.json'),{'expected_root':expected,'frozen_before_model':True,
            'injection':'Harness-only direct state change in owned containers; never an Agent tool','scope':scope})
        async with httpx.AsyncClient(timeout=325,trust_env=False) as client:
            response=await client.post(self.agent_url+'/agent/ops/investigations',json=body,headers={'X-Ops-Token':self.ops_admin})
        try:output=response.json()
        except ValueError:output={'state':'failed','error':'NON_JSON_RESPONSE'}
        write(self.path/('live-'+identifier+'.json'),{'request':body,'http_status':response.status_code,'output':output,'expected_root':expected})
        passed=output.get('root_code')==expected and output.get('answer_origin')=='llm_grounded' and output.get('state')=='completed'
        self.stage('live-'+identifier+('-PASS' if passed else '-FAIL'))
        return passed,body,output

    async def capture(self,identifier,voucher,start=None):
        from ops.tools import ObservationScope,LiveProvider,TOOLS
        end=int(time.time()*1000);scope=ObservationScope(start=start or end-1500,end=end,voucher_id=voucher)
        async with httpx.AsyncClient(base_url=self.java_url,timeout=8,trust_env=False) as client:
            provider=LiveProvider(client,self.ops_java);samples={}
            for tool in TOOLS:
                try:samples[tool]=[await provider.read(tool,scope)]
                except Exception as exc:samples[tool]=[{'_error':type(exc).__name__,'status':exc.response.status_code if isinstance(exc,httpx.HTTPStatusError) else None}]
            await asyncio.sleep(.5)
            samples['inventory'].append(await provider.read('inventory',scope))
        data={'scope':scope.model_dump(),'samples':samples,'origin':'Real Java read-only HTTP; captured before evaluation; no generated business values'}
        write(self.path/('snapshot-'+identifier+'.json'),data)
        return data

    async def auth_checks(self):
        end=int(time.time()*1000);params={'start':end-1000,'end':end,'voucherId':95001}
        proof={}
        async with httpx.AsyncClient(timeout=5,trust_env=False) as c:
            for label,headers in [('missing',{}),('wrong',{'X-Ops-Token':'invalid'}),('consumer',{'authorization':self.tokens[10001]})]:
                proof['java_'+label]=(await c.get(self.java_url+'/internal/ops/inventory',params=params,headers=headers)).status_code
                proof['agent_'+label]=(await c.get(self.agent_url+'/agent/ops/access',headers=headers)).status_code
            proof['java_write_method']=(await c.post(self.java_url+'/internal/ops/inventory',params=params,headers={'X-Ops-Token':self.ops_java})).status_code
            proof['agent_extra_command']=(await c.post(self.agent_url+'/agent/ops/investigations',headers={'X-Ops-Token':self.ops_admin},json={
                'symptom':'run command','scope':{'start':end-1000,'end':end,'voucher_id':95001},'shell':'docker restart mysql'})).status_code
        write(self.path/'auth_checks.json',proof)
        assert all(v==403 for k,v in proof.items() if k.endswith(('missing','wrong','consumer')))
        assert proof['java_write_method']==405 and proof['agent_extra_command']==422

    async def submit(self,voucher,uid=10001):
        async with httpx.AsyncClient(base_url=self.java_url,timeout=10,trust_env=False) as client:
            response=await client.post('/voucher-order/seckill/'+str(voucher),headers={'authorization':self.tokens[uid]})
        return {'http':response.status_code,'body':response.json() if response.content else None}

    async def stock_fault(self):
        await self.coupon(95001,10)
        async with Redis.from_url(self.redis_url,decode_responses=True) as redis:await redis.set('seckill:stock:95001',7)
        write(self.path/'injection-stock.json',{'voucher':95001,'mysql':10,'redis_before':10,'redis_after':7,'orders':0})
        ok,body,out=await self.probe('stock','库存数量对不上，请排除订单在途预扣及补偿后判断原因，并说明还无法知道的事情。',95001,'REDIS_STOCK_DRIFT')
        if not ok:
            self.stage('CORE_NEEDS_FIX')
            for _ in range(180):
                control=self.path/'continue.json'
                if control.exists():control.unlink();break
                await asyncio.sleep(5)
            else:raise RuntimeError('Core correction deadline exceeded')
            await self.start_agent()
            ok,body,out=await self.probe('stock-recheck','库存数量对不上，请排除订单在途预扣及补偿后判断原因，并说明还无法知道的事情。',95001,'REDIS_STOCK_DRIFT')
            if not ok:raise RuntimeError('Live core remains unverified')
        await self.capture('stock',95001)
        self.stage('FIRST_COMPLETE_LIVE_CLOSURE_PASS')

    async def healthy(self):
        await self.auth_checks()
        # Normal successful order registers actual consumer offsets before a backlog injection.
        await self.coupon(95004,10);await self.clear_stage_window()
        start=int(time.time()*1000)
        initial=await self.submit(95004);assert initial['body']['success'],initial
        await self.wait_orders(95004,1);await asyncio.sleep(6)
        await self.capture('healthy',95004,start=start)
        await self.capture('missing-history',95004,start=int(time.time()*1000)-600000)

    async def mq_fault(self):
        # Dedicated consumer-group configuration mutation is confined to the fault harness.
        await self.coupon(95002,10)
        disabled=self.admin(['updateSubGroup','-n','namesrv:9876','-b',f'127.0.0.1:{self.broker_port}','-g','hmdp-seckill-order-consumer','-s','false'],'inject-disable-consume')
        assert disabled.returncode==0,'updateSubGroup failed'
        try:
            await self.clear_stage_window()
            start=int(time.time()*1000)
            submits=[await self.submit(95002,uid) for uid in (10002,10003,10004)]
            assert all(s['body']['success'] for s in submits),submits
            await asyncio.sleep(3)
            business=await self.snapshot_voucher(95002)
            write(self.path/'injection-mq.json',{'setting':'consumeEnable=false','actual_requests':submits,'business':business})
            snapshot=await self.capture('mq',95002,start=start)
            mq=snapshot['samples']['mq'][0].get('data',{})
            assert mq.get('consumeEnabled') is False and mq.get('lag',0)>=3 and not business['orders'],mq
            await self.probe('mq','订单已接收但一直没有落库，请调查是生产失败、消费故障还是库存不足。',95002,'MQ_CONSUME_DISABLED',snapshot['scope'])
        finally:
            restored=self.admin(['updateSubGroup','-n','namesrv:9876','-b',f'127.0.0.1:{self.broker_port}','-g','hmdp-seckill-order-consumer','-s','true'],'restore-consume')
            assert restored.returncode==0,'consume switch restoration failed'
        await self.wait_orders(95002,3)
        write(self.path/'mq-restored.json',await self.snapshot_voucher(95002))

    async def duplicate_fault(self):
        # Publish an actual second copy of an already consumed order body.
        await self.coupon(95003,10);await self.clear_stage_window()
        start=int(time.time()*1000)
        submitted=await self.submit(95003,10005);assert submitted['body']['success'];await self.wait_orders(95003,1)
        oid=str(submitted['body']['data']['orderId'])
        message=self.sql(f"SELECT message_body FROM tb_mq_message WHERE order_id={int(oid)} AND topic='seckill-order-topic';")[0][0]
        libs=self.path/'temp/java-libs';libs.mkdir()
        with zipfile.ZipFile(self.jar) as archive:
            for member in archive.namelist():
                if member.startswith('BOOT-INF/lib/') and member.endswith('.jar'):
                    (libs/Path(member).name).write_bytes(archive.read(member))
        classpath=str(ROOT/'target/classes')+';'+str(libs/'*')
        self.command(['javac','-encoding','UTF-8','-cp',classpath,'-d',self.path/'temp',ROOT/'tests/real_e2e/ReplayOrder.java'])
        published=self.command(['java','-Xmx128m','-XX:MaxDirectMemorySize=64m','-Dfile.encoding=UTF-8',
            '-Duser.home='+str(self.path/'temp/java-home'),'-Drocketmq.client.logRoot='+str(self.path/'temp/mq-client-logs'),
            '-cp',str(self.path/'temp')+';'+classpath,'ReplayOrder',f'127.0.0.1:{self.namesrv_port}','seckill-order-topic',message,self.id],timeout=40,check=False)
        (self.path/'inject-duplicate.log').write_text(published.stdout+published.stderr,encoding='utf-8')
        assert published.returncode==0 and 'VALIDATION_SEND SEND_OK' in published.stdout
        await asyncio.sleep(3);snapshot=await self.snapshot_voucher(95003)
        write(self.path/'injection-duplicate.json',{'order_id':oid,'actual_post_replay_business':snapshot})
        assert len(snapshot['orders'])==1 and snapshot['mysql_stock']==snapshot['redis_stock']==9
        frozen=await self.capture('duplicate',95003,start=start)
        deliveries=frozen['samples']['delivery'][0]['data']['events']
        assert sum(e['type']=='mq_received' and str(e.get('orderId'))==oid for e in deliveries)==2
        await self.probe('duplicate','同一订单似乎被反复处理，请核对是否重复投递、是否重复落库。',95003,'DUPLICATE_DELIVERY',frozen['scope'])

    async def ip_fault(self):
        # Exactly 31 serial admission probes, not a concurrency capacity benchmark.
        await self.coupon(95005,100,future=True);await self.clear_stage_window()
        start=int(time.time()*1000)
        probes=[await self.submit(95005,10001+i) for i in range(31)]
        write(self.path/'injection-ip.json',{'purpose':'Single 30/minute IP threshold reproduction; no concurrency/throughput claim','requests':probes})
        assert probes[-1]['http']==429 and all(p['http']==200 for p in probes[:-1])
        frozen=await self.capture('ip',95005,start=start)
        assert any(e.get('status')==429 and e.get('limitScope')=='ip' for e in frozen['samples']['traffic'][0]['data']['events'])
        await self.probe('ip','多个账号同一出口抢券出现429，请区分用户限流、IP限流及服务异常。',95005,'IP_RATE_LIMIT',frozen['scope'])


async def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--resume-stock',type=Path)
    args=parser.parse_args()
    identifier=datetime.now().strftime('v03-%Y%m%d-%H%M%S-')+uuid.uuid4().hex[:8]
    path=ROOT/'artifacts/test-runs'/identifier;(path/'temp').mkdir(parents=True);(path/'.run-owner').write_text(identifier)
    write(path/'resource_manifest.json',{'run_id':identifier,'created_at':datetime.now().isoformat(),'compose_project':'sltest-'+identifier,'resources':[]})
    write(path/'authorization.json',{'stage':'P4 AIOps','endpoint':'api.openlux.ai','total_token_cap':None,
        'source':'User authorizes real model and isolated fault injection; no automatic remediation','per_investigation_model_requests':8,'steps':3,'tool_reads':8,'timeout_seconds':300})
    run=P4Run(path);run.register('directory',str(path/'temp'),'Dedicated isolated transient data')
    run.ops_admin=secrets.token_urlsafe(32);run.ops_java=secrets.token_urlsafe(32)
    run.env.update(OPS_ADMIN_TOKEN=run.ops_admin,OPS_JAVA_TOKEN=run.ops_java,HMDP_OPS_ADMIN_TOKEN=run.ops_java)
    write(ROOT/'artifacts/p4-current.json',{'run':str(path)})
    print('P4_RUN '+str(path),flush=True)
    try:
        run.sample('before');run.build();await run.infrastructure()
        run.monitor_thread=threading.Thread(target=run.monitor,daemon=True);run.monitor_thread.start()
        await run.start_agent()
        if args.resume_stock:
            prior=args.resume_stock.resolve()
            assert prior.parent==path.parent and load(prior/'live-stock.json')['output']['root_code']=='REDIS_STOCK_DRIFT'
            assert (prior/'snapshot-stock.json').is_file()
            write(path/'continued_from.json',{'stock_source':str(prior)})
        else:
            await run.stock_fault()
        await run.healthy()
        scenario_errors={}
        for name,scenario in [('mq',run.mq_fault),('duplicate',run.duplicate_fault),('ip',run.ip_fault)]:
            try:
                await scenario()
            except Exception as exc:
                scenario_errors[name]=traceback.format_exc()
                write(path/'scenario_errors.json',scenario_errors)
                run.stage(f'{name}-failed-{type(exc).__name__}')
        run.sample('all-faults-captured',guard=True)
        if scenario_errors:run.failure='Some fault captures failed; see scenario_errors.json'
    except BaseException as exc:
        run.failure=type(exc).__name__+': '+str(exc);write(path/'failure.json',{'error':run.failure,'traceback':traceback.format_exc()});print(run.failure,flush=True)
    finally:
        if run.env.get('AGENT_TASK_DB'):run.backup_tasks()
        run.cleanup()
        write(path/'capture_state.json',{'failure':run.failure,'phase':'real isolation and live closure'})
    print('P4_CAPTURE_FINISHED '+str(path),flush=True)


if __name__=='__main__':asyncio.run(main())
