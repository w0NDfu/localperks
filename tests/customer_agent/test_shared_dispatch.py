"""Shared queue and quotas use real isolated PostgreSQL/Redis, never paid HTTP."""
import asyncio
import hashlib
import json
import os
import uuid
import subprocess
import sys
from pathlib import Path
from dataclasses import replace

import httpx
import pytest
from redis.asyncio import Redis

from api.shared_dispatch import SharedDispatcher
from core.config import Settings
from core.state import AgentError
from memory.credentials import CredentialVault
from memory.notifications import EventHub
from memory.profiles import ProfileStore
from memory.task_store import TaskStore
from models.runtime import Quota


async def wait_until(check, timeout=20):
    end=asyncio.get_running_loop().time()+timeout
    while not await check():
        assert asyncio.get_running_loop().time()<end, 'Shared task did not converge'
        await asyncio.sleep(.02)


def settings(**kwargs):
    return Settings(api_key='',state_dsn=os.environ['AGENT_TEST_STATE_DSN'],**kwargs)


def test_two_instances_consume_one_fifo_and_share_global_worker_budget():
    async def run():
        stores=[TaskStore(os.environ['AGENT_TEST_STATE_DSN']) for _ in range(2)]
        clients=[Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'],decode_responses=True) for _ in range(2)]
        hubs=[EventHub(s) for s in stores]
        vaults=[CredentialVault(r,os.environ['AGENT_CREDENTIAL_KEY'],s.schema) for r,s in zip(clients,stores)]
        active=0;peak=0;occupied=set();observed=[];dispatchers=[];consumers=set()
        def execute(index):
            async def work(job,token):
                nonlocal active,peak
                session=job['session_id']
                assert session not in occupied
                assert token=='private-login'
                occupied.add(session);active+=1;peak=max(peak,active);consumers.add(index)
                await stores[index].claim(job['scope'],job['task_id'],admitted=True)
                await asyncio.sleep(.03)
                observed.append((session,json.loads(job['payload'])['message']))
                await stores[index].finish(job['task_id'],'completed',{'answer':'ok'})
                active-=1;occupied.remove(session)
            return work
        for i in range(2):
            dispatchers.append(SharedDispatcher(stores[i],vaults[i],hubs[i],execute(i),workers=2,global_workers=3,per_user=100))
        try:
            for hub in hubs:await hub.start()
            # Admission instance need not be alive for an execution instance to find the task.
            for i in range(30):
                await dispatchers[i%2].submit('user:1',str(uuid.uuid4()),str(i),{'message':str(i)},'session-'+str(i%6),'private-login')
            queued=await stores[0].call(lambda c:c.execute('SELECT session_id,payload FROM agent_jobs ORDER BY ordinal').fetchall())
            for dispatcher in dispatchers:await dispatcher.start()
            async def done():
                return not await stores[0].call(lambda c:c.execute("SELECT task_id FROM agent_jobs WHERE state IN ('queued','running') LIMIT 1").fetchone())
            await wait_until(done)
            assert 1<peak<=3 and consumers=={0,1}
            for session in {x['session_id'] for x in queued}:
                assert [v for s,v in observed if s==session]==[json.loads(x['payload'])['message'] for x in queued if x['session_id']==session]
            dump=await stores[0].call(lambda c:c.execute('SELECT request FROM agent_tasks').fetchall())
            assert 'private-login' not in json.dumps(dump)
            assert len(observed)==30
        finally:
            for d in dispatchers:await d.close()
            for h in hubs:await h.close()
            for r in clients:await r.aclose()
    asyncio.run(run())


def test_replica_startup_does_not_repeat_ddl_while_the_queue_is_running():
    async def run():
        first=TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        # Active writers hold this compatible lock. Startup must only read migration metadata.
        with first.connect() as conn:
            conn.execute('LOCK TABLE agent_jobs IN ROW EXCLUSIVE MODE')
            second=await asyncio.wait_for(asyncio.to_thread(TaskStore,os.environ['AGENT_TEST_STATE_DSN']),2)
            assert await second.ready()
    asyncio.run(run())


def test_expired_job_is_fenced_and_cannot_skip_an_earlier_session_turn():
    async def run():
        a,b=TaskStore(os.environ['AGENT_TEST_STATE_DSN']),TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        r=Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'],decode_responses=True)
        v=CredentialVault(r,os.environ['AGENT_CREDENTIAL_KEY'],a.schema)
        async def unused(*args):pass
        qa=SharedDispatcher(a,v,EventHub(a),unused);qb=SharedDispatcher(b,v,EventHub(b),unused)
        try:
            first,_=await qa.submit('user:1','one','one',{'message':'first'},'s','private-login')
            await qa.submit('user:1','two','two',{'message':'second'},'s','private-login')
            old=await qa.claim_next()
            await a.claim('user:1',first['id'],admitted=True)
            assert await qb.claim_next() is None
            await a.call(lambda c:c.execute('UPDATE agent_jobs SET lease_until=0 WHERE task_id=%s',(first['id'],)))
            await a.call(lambda c:c.execute('UPDATE agent_tasks SET lease_until=0 WHERE id=%s',(first['id'],)))
            takeover=await qb.claim_next()
            assert takeover['task_id']==old['task_id'] and takeover['owner']!=old['owner']
            await b.claim('user:1',first['id'],admitted=True)
            with pytest.raises(AgentError,match='租约'):
                await a.finish(first['id'],'completed',{'wrong':True})
            assert await qa._finish(old,'done')==0
            await b.finish(first['id'],'completed',{'answer':'right'})
            assert await qb._finish(takeover,'done')==1
            assert json.loads((await qb.claim_next())['payload'])['message']=='second'
        finally:await r.aclose()
    asyncio.run(run())


def test_credentials_are_encrypted_scoped_expiring_and_reauthentication_resumes_original_id():
    async def run():
        s=TaskStore(os.environ['AGENT_TEST_STATE_DSN']);r=Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'],decode_responses=True)
        v=CredentialVault(r,os.environ['AGENT_CREDENTIAL_KEY'],s.schema);h=EventHub(s)
        async def execute(job,token):
            assert token=='refreshed-login'
            await s.claim(job['scope'],job['task_id'],admitted=True)
            await s.finish(job['task_id'],'completed',{'answer':'ok'})
        q=SharedDispatcher(s,v,h,execute,workers=1)
        try:
            receipt,_=await q.submit('user:1','request','fingerprint',{'message':'x'},None,'private-login')
            row=await s.call(lambda c:c.execute('SELECT credential_ref FROM agent_jobs').fetchone())
            cipher=await r.get(v.key(row['credential_ref']))
            assert 'private-login' not in cipher and await r.ttl(v.key(row['credential_ref']))>0
            with pytest.raises(AgentError):await v.get('user:2',row['credential_ref'])
            await v.delete(row['credential_ref'])
            await h.start();await q.start()
            async def expired():return (await s.get('user:1',receipt['id']))['state']=='waiting_auth'
            await wait_until(expired)
            duplicate,created=await q.submit('user:1','request','fingerprint',{'message':'x'},None,'refreshed-login')
            assert duplicate['id']==receipt['id'] and not created
            async def done():return (await s.get('user:1',receipt['id']))['state']=='completed'
            await wait_until(done)
        finally:await q.close();await h.close();await r.aclose()
    asyncio.run(run())


def test_global_queue_limit_rejects_atomically_and_duplicate_receipt_still_works():
    async def run():
        s=TaskStore(os.environ['AGENT_TEST_STATE_DSN']);r=Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'],decode_responses=True)
        v=CredentialVault(r,os.environ['AGENT_CREDENTIAL_KEY'],s.schema)
        q=SharedDispatcher(s,v,EventHub(s),None,max_pending=1)
        try:
            first,_=await q.submit('user:1','one','fp',{'message':'x'},None,'login')
            duplicate,_=await q.submit('user:1','one','fp',{'message':'x'},None,'login')
            assert first['id']==duplicate['id']
            with pytest.raises(AgentError,match='队列已满'):
                await q.submit('user:2','two','fp2',{'message':'y'},None,'login')
            assert await s.call(lambda c:c.execute('SELECT COUNT(*) AS n FROM agent_tasks').fetchone()['n'])==1
            await s.call(lambda c:c.execute("UPDATE agent_jobs SET state='expired' WHERE task_id=%s",(first['id'],)))
            await q.submit('user:2','two','fp2',{'message':'y'},None,'login')
            with pytest.raises(AgentError,match='队列已满'):
                await q.submit('user:1','one','fp',{'message':'x'},None,'login')
        finally:await r.aclose()
    asyncio.run(run())


def test_model_rpm_tpm_and_inflight_are_shared_and_unknown_usage_keeps_reservation():
    async def run():
        r=Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'],decode_responses=True)
        cfg=settings(model_rpm=2,model_tpm=100,model_concurrency=2,model_quota_wait=0)
        a,b=Quota(r,cfg),Quota(r,cfg)
        try:
            first=await a.reserve('model',60)
            assert first and await b.reserve('model',50) is None
            await a.release('model',first,20)
            second=await b.reserve('model',50)
            assert second and await a.reserve('model',1) is None
            await b.release('model',second)
            assert int(await r.hget(a.keys('model')[2],'used'))==70
            fast=replace(cfg,model_rpm=100,model_tpm=10000,model_concurrency=3)
            gates=[Quota(r,fast),Quota(r,fast)]
            attempts=await asyncio.gather(*(gates[i%2].reserve('another-model',10) for i in range(30)))
            assert len([x for x in attempts if x])==3
            for item in filter(None,attempts):await gates[0].release('another-model',item)
        finally:await r.aclose()
    asyncio.run(run())


@pytest.mark.parametrize('change',[{'redis_connections':0},{'redis_pool_wait':0},{'model_call_timeout':0},{'model_call_timeout':float('inf')}])
def test_invalid_connection_and_model_time_budgets_fail_at_configuration(change):
    with pytest.raises(ValueError):settings(**change)


def test_notifications_cross_instances_without_per_socket_database_polling():
    async def run():
        a,b=TaskStore(os.environ['AGENT_TEST_STATE_DSN']),TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
        hub=EventHub(b)
        task=await a.create('user:1','session',{'message':'x'})
        try:
            await hub.start()
            async with hub.watch(task) as signal:
                await a.event(task,{'type':'progress'})
                await asyncio.wait_for(signal.wait(),3)
                events=await b.events('user:1',task)
                assert events[-1]['type']=='progress'
                signal.clear()
                with pytest.raises(TimeoutError):await asyncio.wait_for(signal.wait(),.2)
        finally:await hub.close()
    asyncio.run(run())


def test_independent_processes_share_session_fifo_and_global_execution_limit():
    async def run():
        s=TaskStore(os.environ['AGENT_TEST_STATE_DSN']);r=Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'],decode_responses=True)
        v=CredentialVault(r,os.environ['AGENT_CREDENTIAL_KEY'],s.schema)
        q=SharedDispatcher(s,v,EventHub(s),None,per_user=100)
        flag=Path(os.environ['PERSISTENCE_EVIDENCE'])/('workers-'+uuid.uuid4().hex)
        await s.call(lambda c:c.execute('CREATE TABLE worker_probe(task_id TEXT PRIMARY KEY,session_id TEXT,pid INTEGER,finished BOOLEAN DEFAULT FALSE)'))
        await s.call(lambda c:c.execute('CREATE TABLE worker_peak(peak INTEGER); INSERT INTO worker_peak VALUES(0)'))
        for i in range(30):
            await q.submit('user:1',str(i),str(i),{'message':str(i)},'s'+str(i%6),'synthetic-login')
        code='''import asyncio,os,sys,json
from pathlib import Path
from redis.asyncio import Redis
from memory.task_store import TaskStore
from memory.notifications import EventHub
from memory.credentials import CredentialVault
from api.shared_dispatch import SharedDispatcher
async def main():
 s=TaskStore(os.environ['AGENT_TEST_STATE_DSN']);r=Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'],decode_responses=True)
 h=EventHub(s);v=CredentialVault(r,os.environ['AGENT_CREDENTIAL_KEY'],s.schema)
 async def execute(job,token):
  assert token=='synthetic-login'
  await s.claim(job['scope'],job['task_id'],admitted=True)
  def begin(c):
   assert not c.execute('SELECT task_id FROM worker_probe WHERE session_id=%s AND NOT finished',(job['session_id'],)).fetchone()
   c.execute('INSERT INTO worker_probe(task_id,session_id,pid) VALUES(%s,%s,%s)',(job['task_id'],job['session_id'],os.getpid()))
   c.execute("UPDATE worker_peak SET peak=GREATEST(peak,(SELECT COUNT(*) FROM agent_jobs WHERE state='running'))")
  await s.call(begin);await asyncio.sleep(.05)
  await s.finish(job['task_id'],'completed',{'answer':'ok'})
  await s.call(lambda c:c.execute('UPDATE worker_probe SET finished=TRUE WHERE task_id=%s',(job['task_id'],)))
 q=SharedDispatcher(s,v,h,execute,workers=2,global_workers=3,per_user=100)
 try:
  await h.start();print('READY',flush=True)
  while not Path(sys.argv[1]).exists():await asyncio.sleep(.01)
  await q.start()
  deadline=asyncio.get_running_loop().time()+25
  while await s.call(lambda c:c.execute("SELECT task_id FROM agent_jobs WHERE state IN ('queued','running') LIMIT 1").fetchone()):
   assert asyncio.get_running_loop().time()<deadline
   await asyncio.sleep(.05)
 finally:await q.close();await h.close();await r.aclose();await s.aclose()
asyncio.run(main())'''
        children=[]
        try:
            for _ in range(2):
                children.append(subprocess.Popen([sys.executable,'-c',code,str(flag)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8',env=os.environ.copy()))
            ready=await asyncio.gather(*(asyncio.wait_for(asyncio.to_thread(p.stdout.readline),15) for p in children))
            assert ready==['READY\n','READY\n']
            flag.write_text('start')
            outputs=await asyncio.gather(*(asyncio.to_thread(p.communicate,timeout=35) for p in children))
            assert all(p.returncode==0 for p in children),outputs
            metrics=await s.call(lambda c:c.execute('SELECT COUNT(*) AS n,COUNT(DISTINCT pid) AS processes,BOOL_AND(finished) AS complete FROM worker_probe').fetchone())
            peak=await s.call(lambda c:c.execute('SELECT peak FROM worker_peak').fetchone()['peak'])
            assert metrics['n']==30 and metrics['processes']==2 and metrics['complete'] and peak<=3
        finally:
            for p in children:
                if p.poll() is None:p.kill();p.wait(timeout=5)
            if flag.exists():flag.unlink()
            await r.aclose()
    asyncio.run(run())
