import argparse
import asyncio
import json
import os
import sys
import tempfile
import time
import zipfile
from pathlib import Path

HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[1];AGENT=ROOT / 'agent-service'
sys.path.insert(0,str(HERE))
from usage import ActualApiTransport,CASE


async def baseline(args):
    with tempfile.TemporaryDirectory(dir=Path(args.run)/'temp',prefix='original-') as directory:
        with zipfile.ZipFile(AGENT/'evaluations/baseline_v02.zip') as archive:
            for member in archive.infolist():
                if not (Path(directory)/member.filename).resolve().is_relative_to(Path(directory).resolve()):raise ValueError('Unsafe archive')
            archive.extractall(directory)
        sys.path.insert(0,directory)
        from core.config import load_settings
        from dataclasses import replace
        from main import create_app
        import httpx
        CASE.set(args.case)
        app=create_app(settings=replace(load_settings(),turn_timeout=200,max_rounds=5),model_transport=ActualApiTransport(args.run))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app,raise_app_exceptions=False),base_url='http://original') as client:
                started=time.perf_counter()
                response=await client.post('/agent/chat/stream',json={'message':args.message},headers={'authorization':os.environ['EVAL_LOGIN_TOKEN']})
                events=[json.loads(line[6:]) for line in response.text.splitlines() if line.startswith('data: ')]
                data=next((e for e in reversed(events) if e.get('type')=='done'),{'trace':events,'error':next((e for e in events if e.get('type')=='error'),'INCOMPLETE')})
                data.update(http_status=response.status_code,latency_ms=(time.perf_counter()-started)*1000)
                return data


def server(args):
    import httpx
    import uvicorn
    from fastapi import Request
    from fastapi.responses import Response,JSONResponse
    sys.path.insert(0,str(AGENT))
    from core.config import load_settings
    from dataclasses import replace
    from main import create_app
    from rag.retrieval import RagService,EmbeddingClient,Reranker
    cfg=replace(load_settings(),turn_timeout=200,max_rounds=5)
    class JavaTransport(httpx.AsyncHTTPTransport):
        async def handle_async_request(self,request):
            request.headers['X-Eval-Case']=CASE.get()
            return await super().handle_async_request(request)
    app=create_app(settings=cfg,transport=JavaTransport(),model_transport=ActualApiTransport(args.run))
    @app.middleware('http')
    async def tag(request:Request,call_next):
        token=CASE.set(request.headers.get('X-Eval-Case','untagged'))
        try:return await call_next(request)
        finally:CASE.reset(token)
    failures=set()
    @app.api_route('/evaluation-java/{path:path}',methods=['GET','POST'])
    async def proxy(path:str,request:Request):
        case=CASE.get()
        fault=(case=='fault_partial' and path=='voucher/list/9003') or (case=='fault_recover' and path=='shop-type/list' and case not in failures)
        if fault:
            failures.add(case)
            return JSONResponse(status_code=503,content={'success':False,'errorMsg':'Explicit evaluation network fault injection'})
        async with httpx.AsyncClient(timeout=8,trust_env=False) as client:
            response=await client.request(request.method,os.environ['EVAL_REAL_JAVA']+'/'+path,params=request.query_params,
                headers={'authorization':request.headers.get('authorization','')},content=await request.body())
        return Response(content=response.content,status_code=response.status_code,media_type='application/json')
    uvicorn.run(app,host='127.0.0.1',port=args.port,log_level='warning')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['server','baseline']);p.add_argument('--run',required=True);p.add_argument('--port',type=int);p.add_argument('--case');p.add_argument('--message');a=p.parse_args()
    if a.mode=='server':server(a)
    else:
        try:print(json.dumps(asyncio.run(baseline(a)),ensure_ascii=False))
        except Exception as exc:print(json.dumps({'http_status':0,'error':type(exc).__name__,'latency_ms':0}))
