import asyncio
import json
import time
import hashlib
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any
from uuid import UUID

import httpx
from fastapi import FastAPI, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis, BlockingConnectionPool
from redis.exceptions import RedisError

from core.config import Settings, load_settings
from core.context import ToolContext
from core.engine import AgentEngine
from core.state import AgentError
from memory.redis_memory import RedisMemory
from models.llm_client import open_model
from tools.java_api import JavaAPI, ShopLinkError
from tools.shop_tools import search_shops
from orchestration.state import ConsumptionConstraints
from orchestration.supervisor import Supervisor
from memory.task_store import TaskStore
from api.actions import ActionService, ActionRequest, ActionConfirmation
from rag.retrieval import RagService, EmbeddingClient, Reranker
from observability.metrics import summarize
from core.hooks import Harness
from api.shared_dispatch import SharedDispatcher
from memory.credentials import CredentialVault
from memory.notifications import EventHub
from memory.profiles import ProfileStore, ProfileUpdate
from models.runtime import ModelRuntime, RuntimeMiddleware


@dataclass
class ShopSearchResult:
    page: int
    data: list[dict[str, Any]]


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    message: str = Field(min_length=1, max_length=4000)
    session_id: UUID | None = None
    constraints: ConsumptionConstraints | None = None
    task_id: UUID | None = None


class SubmitRequest(ChatRequest):
    request_id: UUID


def create_app(*, transport: httpx.AsyncBaseTransport | None = None,
               model_transport: httpx.AsyncBaseTransport | None = None,
               memory=None, settings: Settings | None = None, task_store=None, rag_service=None) -> FastAPI:
    def config():
        return settings or load_settings()

    @asynccontextmanager
    async def lifespan(app):
        current = config()
        # Fail before allocating clients/pools if the deployment key is missing or malformed.
        from cryptography.fernet import Fernet
        if not current.credential_key:
            raise ValueError('Run python -m api.bootstrap to configure AGENT_CREDENTIAL_KEY')
        Fernet(current.credential_key.encode())
        async with httpx.AsyncClient(base_url=current.shoplink_base_url,
                                    timeout=httpx.Timeout(5, connect=2),
                                    transport=transport, trust_env=False) as client:
            redis = Redis(connection_pool=BlockingConnectionPool.from_url(current.redis_url,decode_responses=True,
                max_connections=current.redis_connections,timeout=current.redis_pool_wait,
                socket_connect_timeout=2,socket_timeout=3))
            app.state.shoplink_client = client
            app.state.memory = memory or RedisMemory(redis)
            app.state.redis = redis
            app.state.task_store = task_store or await asyncio.to_thread(TaskStore, current.state_dsn, schema=current.state_schema)
            app.state.rag = rag_service or RagService(current, embedding=EmbeddingClient(current, model_transport), reranker=Reranker(current, model_transport), model_transport=model_transport)
            app.state.turn_limit = asyncio.Semaphore(current.worker_concurrency)
            app.state.harness = Harness()
            app.state.profiles = ProfileStore(app.state.task_store)
            app.state.event_hub = EventHub(app.state.task_store)
            app.state.model_runtime = ModelRuntime(current,redis,model_transport)
            vault=CredentialVault(redis,current.credential_key,app.state.task_store.schema,current.credential_ttl)

            async def execute(job, authorization):
                from types import SimpleNamespace
                proxy=SimpleNamespace(app=app)
                try:
                    owner,ctx=await identity(proxy,authorization)
                except AgentError as exc:
                    if exc.status_code==401:
                        raise AgentError('REAUTH_REQUIRED','登录已过期，请重新登录后重交原请求',401) from exc
                    raise
                if owner!=job['scope']:
                    raise AgentError('REAUTH_REQUIRED','身份已变化，请重新登录后重交原请求',401)
                payload=json.loads(job['payload'])
                execution=ChatRequest(message=payload['message'],session_id=UUID(job['session_id']),constraints=payload.get('constraints'))
                try:
                    await app.state.memory.load(owner,job['session_id'])
                except AgentError as exc:
                    if exc.code!='SESSION_NOT_FOUND':
                        raise
                    previous=await app.state.task_store.get(owner,job['task_id'])
                    await app.state.memory.save(owner,job['session_id'],previous['request'].get('history',[]))
                async def ignore(_event):
                    pass
                with app.state.model_runtime.activate():
                    # Shared workers already own their bounded execution slot. A legacy
                    # request must not stall a claimed job outside its workflow timeout.
                    try:
                        await run_workflow(execution,proxy,current,owner,ctx,ignore,admitted_task_id=job['task_id'])
                    except AgentError as exc:
                        latest=await app.state.task_store.get(owner,job['task_id'])
                        if latest['state']=='accepted' and exc.code not in ('SESSION_BUSY','TASK_BUSY'):
                            await app.state.task_store.finish(job['task_id'],'failed',{'error':{'code':exc.code,'message':str(exc)}})
                        raise

            app.state.dispatcher=SharedDispatcher(app.state.task_store,vault,app.state.event_hub,execute,
                workers=current.worker_concurrency,max_pending=current.queue_capacity,per_user=current.per_user_pending,
                global_workers=current.global_workflow_concurrency,max_wait=current.queue_wait_seconds,
                lease_seconds=current.turn_timeout+50)
            try:
                await app.state.event_hub.start()
                await app.state.dispatcher.start()
                yield
            finally:
                await app.state.dispatcher.close()
                await app.state.event_hub.close()
                await app.state.model_runtime.close()
                await redis.aclose(close_connection_pool=True)
                if task_store is None:
                    await app.state.task_store.aclose()

    app = FastAPI(title="LocalPerks 多Agent服务", version="0.3.0", lifespan=lifespan)

    app.add_middleware(RuntimeMiddleware)

    @app.exception_handler(AgentError)
    async def agent_error(_request, exc):
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=exc.status_code,
                            content={"detail": {"code": exc.code, "message": str(exc)}})

    @app.exception_handler(RedisError)
    async def redis_error(_request,_exc):
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=503,content={'detail':{'code':'CACHE_UNAVAILABLE','message':'会话服务繁忙或不可用，请稍后重试'}})

    async def identity(request: Request, authorization: str | None):
        token = (authorization or "").strip()
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        api = JavaAPI(request.app.state.shoplink_client, token)
        if not token:
            return "guest", ToolContext(java=api, business_timezone=config().business_timezone, rag=request.app.state.rag)
        try:
            user = await api.get("/user/me", authenticated=True)
        except ShopLinkError as exc:
            raise AgentError(exc.code, str(exc), exc.status_code) from exc
        if not isinstance(user, dict) or not isinstance(user.get("id"), int):
            raise AgentError("LOGIN_REQUIRED", "登录状态已失效，请重新登录", 401)
        scope = "user:" + str(user["id"])
        return scope, ToolContext(java=api, authenticated=True, scope=scope, business_timezone=config().business_timezone, rag=request.app.state.rag)

    async def prepare(body: ChatRequest, request: Request, authorization: str | None):
        current = config()
        if current.orchestration_mode == "legacy" and not current.configured:
            raise AgentError("MODEL_NOT_CONFIGURED",
                             "请在agent-service/.env填写OPENAI_API_KEY和OPENAI_MODEL", 503)
        if not body.message.strip():
            raise AgentError("EMPTY_MESSAGE", "请输入问题", 422)
        scope, ctx = await identity(request, authorization)
        return current, scope, ctx

    async def run(body, request, current, scope, ctx, emit):
        gate = request.app.state.turn_limit
        try:
            await asyncio.wait_for(gate.acquire(), timeout=2)
        except TimeoutError as exc:
            raise AgentError('SERVICE_BUSY', '当前任务并发已满，请稍后重试', 503) from exc
        try:
            return await run_workflow(body, request, current, scope, ctx, emit)
        finally:
            try:
                if body.task_id and ctx.authenticated:
                    try:
                        await asyncio.shield(request.app.state.dispatcher.complete_external(scope,str(body.task_id)))
                    except (AgentError,RedisError):
                        pass
            finally:
                gate.release()

    async def run_workflow(body, request, current, scope, ctx, emit, admitted_task_id=None):
        memory_store = request.app.state.memory
        store = request.app.state.task_store
        task_id = admitted_task_id
        claimed = False
        started = time.perf_counter()
        try:
            async with asyncio.timeout(current.turn_timeout):
                if ctx.authenticated:
                    profile=await request.app.state.profiles.get(scope)
                    ctx.preferences=profile['preferences']
                restored = await store.get(scope, str(body.task_id)) if body.task_id else None
                if restored and body.session_id and str(body.session_id) != restored["session_id"]:
                    raise AgentError("SESSION_CONFLICT", "恢复任务必须使用原会话", 409)
                requested_session = str(body.session_id) if body.session_id else restored["session_id"] if restored else None
                if restored:
                    try:
                        await memory_store.load(scope, requested_session)
                    except AgentError as exc:
                        if exc.code != "SESSION_NOT_FOUND":
                            raise
                        recovered = restored["request"].get("history", [])
                        output = restored["output"] or {}
                        if output.get("answer"):
                            recovered = recovered + [{"user": restored["request"]["message"], "assistant": output["answer"],
                                                       "trace": [], "constraints": output.get("constraints"), "decision_context": output.get("decision_context")}]
                        await memory_store.save(scope, requested_session, recovered)
                async with memory_store.turn(scope, requested_session) as (session_id, history):
                    await emit({"type": "session", "session_id": session_id})
                    trace = []
                    input_constraints = body.constraints.model_dump(exclude_unset=True) if body.constraints else None
                    if restored:
                        prior = (restored["output"] or {}).get("constraints") or restored["request"].get("constraints")
                        if prior:
                            input_constraints = {**prior, **(input_constraints or {})}
                        task_id = str(body.task_id)
                    else:
                        task_id = admitted_task_id or await store.create(scope, session_id, {"message": body.message.strip(), "constraints": input_constraints})
                    message = body.message.strip()
                    if restored:
                        if message in {"继续", "恢复", "重试"}:
                            message = restored["request"]["message"]
                        elif (restored["output"] or {}).get("intent") == "recommend":
                            message = "推荐，补充条件：" + message
                    await store.claim(scope, task_id, {"message": message, "constraints": input_constraints, "history": history[-8:]}, admitted=bool(admitted_task_id))
                    claimed = True
                    await emit({"type": "task", "task_id": task_id, "state": "running"})

                    async def record(event):
                        trace.append(event)
                        await store.event(task_id, event)
                        if event.get("type") == "order_status":
                            from orchestration.state import TaskNode, TaskResult
                            await store.checkpoint(task_id, TaskNode(id="order_watch", agent="order", operation="watch"),
                                                   TaskResult(node_id="order_watch", agent="order", status="completed", data=event["result"]))
                        await emit(event)

                    await request.app.state.harness.check("request", ctx=ctx, message=message, emit=record)

                    if current.orchestration_mode == "legacy":
                        async with open_model(current, model_transport) as model:
                            engine = AgentEngine(model, current.model, current.max_rounds, reasoning_effort=current.reasoning_effort)
                            answer = await engine.run(body.message.strip(), history, ctx, record)
                        outcome = {}
                    else:
                        outcome = await Supervisor(current, model_transport, store=store, task_id=task_id).run(
                            message, history, ctx, record,
                            input_constraints)
                        answer = outcome.pop("answer")
                    # Only complete turns are committed. Token/Authorization never enter memory.
                    history.append({"user": body.message.strip(), "assistant": answer, "trace": trace, "task_id": task_id,
                                    **({"action_id": outcome["action"]["action_id"]} if outcome.get("action") else {}),
                                    **{key: outcome[key] for key in ("constraints", "state", "decision_context", "answer_origin") if key in outcome}})
                    await memory_store.save(scope, session_id, history)
                    result = {"session_id": session_id, "task_id": task_id, "trace_id": task_id,
                              "answer": answer, "trace": trace, **outcome,
                              "metrics": summarize(trace, (time.perf_counter() - started) * 1000)}
                    await store.finish(task_id, outcome.get("state", "completed"), result)
                    await emit({"type": "done", **result})
                    return result
        except TimeoutError as exc:
            if claimed:
                await store.event(task_id, {"type": "error", "code": "TURN_TIMEOUT"})
                await store.finish(task_id, "interrupted", {"error": {"code": "TURN_TIMEOUT", "message": "任务执行超时，可恢复后重新校验实时数据"}})
            raise AgentError("TURN_TIMEOUT", "本轮执行超时，请缩小问题范围", 504) from exc
        except asyncio.CancelledError:
            if claimed:
                await asyncio.shield(store.event(task_id, {"type": "error", "code": "DISCONNECTED"}))
                await asyncio.shield(store.finish(task_id, "interrupted", {"error": {"code": "DISCONNECTED", "message": "连接中断，可恢复任务"}}))
            raise
        except Exception as exc:
            if claimed:
                await store.event(task_id, {"type": "error", "code": getattr(exc, "code", "INTERNAL_ERROR")})
                await store.finish(task_id, "failed", {"error": {"code": getattr(exc, "code", "INTERNAL_ERROR"), "message": str(exc) if isinstance(exc, AgentError) else "任务失败"}})
            raise

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/agent/status")
    async def status(request: Request):
        current = config()
        try:
            await request.app.state.redis.ping()
            redis_ok = True
        except RedisError:
            redis_ok = False
        try:
            await JavaAPI(request.app.state.shoplink_client).get("/shop-type/list")
            java_ok = True
        except ShopLinkError:
            java_ok = False
        store_ok = await request.app.state.task_store.ready()
        return {"configured": current.configured, "model": current.model,
                "base_url": current.base_url, "redis": redis_ok, "localperks": java_ok,
                "ready": redis_ok and java_ok and store_ok, "state_store": store_ok,
                "orchestration": current.orchestration_mode, "workflow_store": "postgresql",
                "async_tasks": True,
                'dispatch_backend':'postgresql','session_order':'durable_admission_fifo',
                'worker_concurrency':current.worker_concurrency,'global_workflow_concurrency':current.global_workflow_concurrency,
                'queue_capacity':current.queue_capacity,'preferences':True,
                'model_budget':{'rpm':current.model_rpm,'tpm':current.model_tpm,'concurrency':current.model_concurrency},
                "rag_configured": bool(current.pgvector_dsn),
                "agents": ["coordinator", "supervisor", "shop", "voucher", "order"]}

    @app.get("/agent/knowledge/search")
    async def knowledge_search(request: Request, query: str = Query(min_length=2, max_length=500), baseline: bool = False):
        return await request.app.state.rag.search(query, baseline=baseline)

    @app.get("/agent/knowledge/sources/{chunk_id}")
    async def knowledge_source(chunk_id: str, request: Request):
        import re
        if not re.fullmatch(r"[a-f0-9]{32}", chunk_id):
            raise AgentError("SOURCE_NOT_FOUND", "知识来源不存在", 404)
        return await request.app.state.rag.store.source(chunk_id)

    @app.get("/agent/tasks/{task_id}")
    async def task_status(task_id: UUID, request: Request, authorization: str | None = Header(default=None)):
        scope, _ctx = await identity(request, authorization)
        task = await request.app.state.task_store.get(scope, str(task_id))
        task["checkpoints"] = await request.app.state.task_store.checkpoints(scope, str(task_id))
        return task

    @app.post("/agent/tasks", status_code=202)
    async def submit(body: SubmitRequest, request: Request, authorization: str | None = Header(default=None)):
        if body.task_id:
            raise AgentError("INVALID_ARGUMENTS", "补充条件请使用恢复任务接口", 422)
        current, scope, ctx = await prepare(body, request, authorization)
        # Anonymous requests are public-only and share no persisted conversation scope.
        # Durable asynchronous conversations require a Java-authenticated identity.
        if not ctx.authenticated:
            raise AgentError("LOGIN_REQUIRED", "异步客服会话请先登录；游客可使用同步公开查询", 401)
        payload = body.model_dump(mode="json", exclude={"request_id", "task_id"})
        fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        receipt,created=await request.app.state.dispatcher.submit(scope,str(body.request_id),fingerprint,payload,
            str(body.session_id) if body.session_id else None,authorization or '')
        task_id,session_id=receipt['id'],receipt['session_id']
        return {"task_id": task_id, "session_id": session_id, "state": receipt["state"],
                "request_id": str(body.request_id), "duplicate": not created}

    @app.websocket("/agent/tasks/{task_id}/ws")
    async def task_socket(task_id: UUID, websocket: WebSocket):
        await websocket.accept()
        try:
            # Credentials belong in the first frame, never in a URL or persisted task.
            hello = await asyncio.wait_for(websocket.receive_json(), timeout=5)
            authorization = hello.get("authorization")
            if not isinstance(authorization, str) or len(authorization) > 4096:
                await websocket.close(code=4401)
                return
            scope, ctx = await identity(websocket, authorization)
            if not ctx.authenticated:
                await websocket.close(code=4401)
                return
            after = hello.get("after", 0)
            if type(after) is not int or after < 0:
                await websocket.close(code=4400)
                return
            store = websocket.app.state.task_store
            async with websocket.app.state.event_hub.watch(str(task_id)) as signal:
                while True:
                    # Clear before reading: a concurrent commit sets it again, avoiding lost wakeups.
                    signal.clear()
                    task = await store.get(scope, str(task_id))
                    events = await store.events(scope, str(task_id), after)
                    for event in events:
                        await websocket.send_json(event)
                        after = event['seq']
                    if len(events)==500:
                        continue
                    if task['state'] not in {'accepted','running'}:
                        await websocket.send_json({'type':'snapshot','state':task['state'],'output':task['output']})
                        await websocket.close()
                        return
                    try:
                        await asyncio.wait_for(signal.wait(),15)
                    except TimeoutError:
                        # Periodic reconciliation also recovers a notification lost during reconnect.
                        await websocket.send_json({'type':'heartbeat','after':after})
        except (WebSocketDisconnect, TimeoutError):
            pass
        except AgentError as exc:
            await websocket.send_json({"type": "error", "code": exc.code, "message": str(exc)})
            await websocket.close(code=4404 if exc.status_code == 404 else 4403)
        except (ValueError, TypeError, AttributeError):
            await websocket.close(code=4400)

    @app.get("/agent/tasks/{task_id}/events")
    async def task_events(task_id: UUID, request: Request, after: int = Query(default=0, ge=0), authorization: str | None = Header(default=None)):
        scope, _ctx = await identity(request, authorization)
        return {"events": await request.app.state.task_store.events(scope, str(task_id), after)}

    @app.post("/agent/tasks/{task_id}/resume")
    async def resume(task_id: UUID, body: ChatRequest, request: Request, authorization: str | None = Header(default=None)):
        body.task_id = task_id
        current, scope, ctx = await prepare(body, request, authorization)
        async def ignore(_event):
            pass
        return await run(body, request, current, scope, ctx, ignore)

    @app.post("/agent/actions/prepare")
    async def prepare_action(body: ActionRequest, request: Request, authorization: str | None = Header(default=None)):
        _scope, ctx = await identity(request, authorization)
        return await ActionService(request.app.state.task_store, config()).prepare(body, ctx)

    @app.post("/agent/actions/{action_id}/confirm")
    async def confirm_action(action_id: UUID, body: ActionConfirmation, request: Request, authorization: str | None = Header(default=None)):
        _scope, ctx = await identity(request, authorization)
        return await ActionService(request.app.state.task_store, config()).confirm(str(action_id), body, ctx)

    @app.get("/agent/actions/{action_id}")
    async def get_action(action_id: UUID, request: Request, authorization: str | None = Header(default=None)):
        _scope, ctx = await identity(request, authorization)
        service = ActionService(request.app.state.task_store, config())
        return service.public(await service.get(str(action_id), ctx))

    @app.post("/agent/actions/{action_id}/reconcile")
    async def reconcile_action(action_id: UUID, request: Request, authorization: str | None = Header(default=None)):
        _scope, ctx = await identity(request, authorization)
        return await ActionService(request.app.state.task_store, config()).reconcile(str(action_id), ctx)

    @app.get('/agent/actions/{action_id}/events')
    async def action_events(action_id: UUID, request: Request, authorization: str | None = Header(default=None)):
        _scope, ctx = await identity(request, authorization)
        return {'events': await ActionService(request.app.state.task_store, config()).events(str(action_id), ctx)}

    @app.post("/agent/chat")
    async def chat(body: ChatRequest, request: Request, authorization: str | None = Header(default=None)):
        current, scope, ctx = await prepare(body, request, authorization)
        async def ignore(_event):
            pass
        return await run(body, request, current, scope, ctx, ignore)

    @app.post("/agent/chat/stream")
    async def stream(body: ChatRequest, request: Request, authorization: str | None = Header(default=None)):
        current, scope, ctx = await prepare(body, request, authorization)

        async def events():
            queue = asyncio.Queue()
            async def produce():
                try:
                    await run(body, request, current, scope, ctx, queue.put)
                except AgentError as exc:
                    await queue.put({"type": "error", "code": exc.code, "message": str(exc)})
                except Exception:
                    # Upstream responses may contain sensitive provider details.
                    import logging
                    logging.getLogger(__name__).error("Agent turn failed; unexpected error")
                    await queue.put({"type": "error", "code": "INTERNAL_ERROR", "message": "服务内部错误，请检查服务配置或稍后重试"})
                finally:
                    await queue.put(None)
            task = asyncio.create_task(produce())
            try:
                while True:
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=10)
                    except TimeoutError:
                        yield ": keep-alive\n\n"
                        continue
                    if event is None:
                        break
                    yield "data: " + json.dumps(event, ensure_ascii=False, default=str) + "\n\n"
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        return StreamingResponse(events(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/agent/conversations/{session_id}")
    async def conversation(session_id: UUID, request: Request, authorization: str | None = Header(default=None)):
        scope, _ctx = await identity(request, authorization)
        return {"session_id": str(session_id),
                "turns": await request.app.state.memory.load(scope, str(session_id))}

    @app.get('/agent/preferences')
    async def preferences(request: Request,authorization: str | None=Header(default=None)):
        scope,_ctx=await identity(request,authorization)
        return await request.app.state.profiles.get(scope)

    @app.put('/agent/preferences')
    async def update_preferences(body: ProfileUpdate,request: Request,authorization: str | None=Header(default=None)):
        scope,_ctx=await identity(request,authorization)
        return await request.app.state.profiles.put(scope,body.preferences.model_dump(exclude_none=True),body.revision)

    @app.delete('/agent/preferences')
    async def delete_preferences(request: Request,revision: int=Query(ge=0),authorization: str | None=Header(default=None)):
        scope,_ctx=await identity(request,authorization)
        return await request.app.state.profiles.delete(scope,revision)

    @app.get("/tools/shops/search", response_model=ShopSearchResult)
    async def shops(request: Request, keyword: Annotated[str, Query(max_length=128)] = "",
                    page: Annotated[int, Query(ge=1, le=2147483647)] = 1):
        try:
            data = await search_shops(request.app.state.shoplink_client, keyword, page)
            return ShopSearchResult(page=page, data=data)
        except ShopLinkError as exc:
            raise HTTPException(exc.status_code, detail={"code": exc.code, "message": str(exc)}) from exc

    if config().ops_enabled:
        from ops.api import install as install_ops
        install_ops(app, config, model_transport)
    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8090)


