import asyncio
import hashlib
import os
import secrets
import time
from uuid import UUID,uuid4

from fastapi import Header,HTTPException,Request,Response
from pydantic import BaseModel,ConfigDict,Field

from ops.engine import Investigator
from ops.case_history import CaseHistory
from ops.tools import ObservationScope,LiveProvider
from ops.usage import UsageTransport,usage_rows


class InvestigationRequest(BaseModel):
    model_config=ConfigDict(extra='forbid')
    symptom: str=Field(min_length=3,max_length=2000)
    scope: ObservationScope


def install(app,config,model_transport=None):
    def administrator(provided):
        token=os.environ.get('OPS_ADMIN_TOKEN','')
        if len(token)<32 or not provided or len(provided)>256 or not secrets.compare_digest(token.encode(),provided.encode()):
            raise HTTPException(403,'需要有效管理员运维凭证')
        return 'ops-admin:'+hashlib.sha256(token.encode()).hexdigest()[:16]

    def no_cache(response):response.headers['Cache-Control']='no-store'

    @app.get('/agent/ops/access')
    async def access(request:Request,response:Response,x_ops_token:str|None=Header(default=None)):
        administrator(x_ops_token);no_cache(response)
        return {'role':'ops_admin','read_only':True,'max_tool_reads':8,'max_model_requests':8,'timeout_seconds':300}

    @app.post('/agent/ops/investigations')
    async def create(body:InvestigationRequest,request:Request,response:Response,x_ops_token:str|None=Header(default=None)):
        scope=administrator(x_ops_token);no_cache(response)
        if body.scope.end>int(time.time()*1000)+5000 or body.scope.start<int(time.time()*1000)-86400000:
            raise HTTPException(422,'只能调查最近一天内的有限时间窗口')
        java_token=os.environ.get('OPS_JAVA_TOKEN',os.environ.get('HMDP_OPS_ADMIN_TOKEN',''))
        if len(java_token)<32:raise HTTPException(503,'Java运维只读凭证未配置')
        settings=config()
        store=request.app.state.task_store
        if app.state.ops_limit.locked():
            raise HTTPException(429,'运维调查并发已满，请稍后再试')
        async with app.state.ops_limit,request.app.state.turn_limit:
            transport=UsageTransport(store,model_transport)
            try:
                task_id=await store.create(scope,str(uuid4()),body.model_dump(mode='json'),kind='ops')
                await store.claim(scope,task_id,lease_seconds=325)
                mode=os.environ.get('OPS_CASE_RAG','rrf').strip().lower()
                history=CaseHistory(settings,transport=transport,rerank=mode=='rerank') if settings.pgvector_dsn and mode in ('rrf','rerank') else None
                result=await Investigator(settings,store,LiveProvider(request.app.state.shoplink_client,java_token),
                    transport,history=history).run(task_id,body.scope,body.symptom,mode='hybrid')
                result['model_usage']=await asyncio.to_thread(usage_rows,store,task_id)
                result['cost']='UNKNOWN'
                return result
            finally:
                if model_transport is None:await transport.inner.aclose()

    @app.get('/agent/ops/investigations')
    async def listing(request:Request,response:Response,x_ops_token:str|None=Header(default=None)):
        scope=administrator(x_ops_token);no_cache(response)
        def query(conn):return [dict(row) for row in conn.execute('SELECT id,state,request,created,updated FROM agent_tasks WHERE scope=%s AND kind=%s ORDER BY created DESC LIMIT 50',(scope,'ops'))]
        return {'items':await request.app.state.task_store.call(query)}

    @app.get('/agent/ops/investigations/{task_id}')
    async def detail(task_id:UUID,request:Request,response:Response,x_ops_token:str|None=Header(default=None)):
        scope=administrator(x_ops_token);no_cache(response)
        store=request.app.state.task_store
        item=await store.get(scope,str(task_id))
        if item['kind']!='ops':raise HTTPException(404,'运维调查不存在')
        return {**item,'events':await request.app.state.task_store.events(scope,str(task_id)),
            'model_usage':await asyncio.to_thread(usage_rows,store,str(task_id)) if item['output'] else [],'cost':'UNKNOWN'}

    @app.post('/agent/ops/investigations/{task_id}/resume')
    async def resume(task_id:UUID,request:Request,response:Response,x_ops_token:str|None=Header(default=None)):
        scope=administrator(x_ops_token);no_cache(response)
        item=await request.app.state.task_store.get(scope,str(task_id))
        if item['kind']!='ops':raise HTTPException(404,'运维调查不存在')
        # Never reuse volatile evidence or invent historical stock. A new linked
        # investigation obtains fresh current samples for the original event window.
        body=InvestigationRequest.model_validate(item['request'])
        await request.app.state.task_store.event(str(task_id),{'type':'ops_resume_requested','policy':'fresh_read_only_observations'})
        result=await create(body,request,response,x_ops_token)
        await request.app.state.task_store.event(result['task_id'],{'type':'ops_resumed_from','parent_task_id':str(task_id)})
        return result

    app.state.ops_limit=asyncio.Semaphore(2)
