"""2000 independent users; real state/Redis, ASGI ingress and a declared model double."""
import asyncio
import json
import math
import os
import time
import uuid
from contextlib import AsyncExitStack
from pathlib import Path

import httpx
import pytest

from core.config import Settings
from main import create_app


@pytest.mark.skipif(os.environ.get('AGENT_TEST_CLUSTER_LOAD')!='1',reason='Enable AGENT_TEST_CLUSTER_LOAD for the isolated 2000-user model-double capacity test')
def test_two_instances_complete_2000_users_without_cross_user_results():
    async def run():
        active=0;peak=0;calls=0
        async def model(request):
            nonlocal active,peak,calls
            payload=json.loads(request.content)
            question=payload['messages'][-1]['content']
            active+=1;peak=max(peak,active);calls+=1
            try:
                await asyncio.sleep(.02)
                return httpx.Response(200,json={'id':'fixture','object':'chat.completion','created':0,'model':'fixture',
                    'choices':[{'index':0,'message':{'role':'assistant','content':'reply:'+question},'finish_reason':'stop'}],
                    'usage':{'prompt_tokens':40,'completion_tokens':10,'total_tokens':50}})
            finally:active-=1
        def java(request):
            if request.url.path=='/user/me':
                return httpx.Response(200,json={'success':True,'data':{'id':int(request.headers['authorization'].split('-')[1])}})
            return httpx.Response(200,json={'success':True,'data':[]})
        cfg=Settings(api_key='explicit-test-fixture',model='fixture',state_dsn=os.environ['AGENT_TEST_STATE_DSN'],
            worker_concurrency=8,global_workflow_concurrency=12,model_concurrency=6,queue_capacity=3000)
        apps=[create_app(settings=cfg,transport=httpx.MockTransport(java),model_transport=httpx.MockTransport(model)) for _ in range(2)]
        async with AsyncExitStack() as stack:
            for app in apps:await stack.enter_async_context(app.router.lifespan_context(app))
            clients=[await stack.enter_async_context(httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://fixture')) for app in apps]
            count=2000;started=time.perf_counter()
            async def submit(i):
                begin=time.perf_counter()
                response=await clients[i%2].post('/agent/tasks',headers={'authorization':f'user-{i+1}'},
                    json={'request_id':str(uuid.uuid4()),'message':f'请回复测试标识 user-{i+1}'})
                assert response.status_code==202,response.text
                return i,response.json()['task_id'],(time.perf_counter()-begin)*1000
            receipts=await asyncio.gather(*(submit(i) for i in range(count)))
            store=apps[0].state.task_store
            deadline=asyncio.get_running_loop().time()+150
            while True:
                rows=await store.call(lambda c:c.execute('SELECT id,scope,state,output FROM agent_tasks').fetchall())
                if len(rows)==count and all(row['state']=='completed' for row in rows):break
                assert asyncio.get_running_loop().time()<deadline,{row['state'] for row in rows}
                await asyncio.sleep(.2)
            ids={task for _,task,_ in receipts}
            assert len(ids)==count and len(rows)==count and calls==count and peak<=6
            for row in rows:
                owner=row['scope'].split(':')[1]
                assert json.loads(row['output'])['answer']==f'reply:请回复测试标识 user-{owner}'
            for index,identifier,_ in receipts[:4]:
                response=await clients[(index+1)%2].get('/agent/tasks/'+identifier,headers={'authorization':f'user-{(index+1)%count+1}'})
                assert response.status_code==404
            durations=sorted(ms for _,_,ms in receipts)
            report={'users':count,'tasks':len(rows),'model_fixture_calls':calls,'real_model_calls':0,
                'ingress':'HTTPX ASGITransport, no TCP load claim','state':'real isolated PostgreSQL and Redis',
                'instances':2,'global_workflow_limit':12,'global_model_limit':6,'observed_model_peak':peak,
                'elapsed_seconds':time.perf_counter()-started,'admission_p95_ms':durations[math.ceil(count*.95)-1],
                'all_user_result_bindings_match':True}
            Path(os.environ['PERSISTENCE_EVIDENCE'],'cluster-2000-users.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    asyncio.run(run())
