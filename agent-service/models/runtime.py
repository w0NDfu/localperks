"""Pooled model HTTP and Redis-atomic account/model sliding-window quotas."""
import asyncio
import contextvars
import hashlib
import json
import uuid
from contextlib import contextmanager, asynccontextmanager

import httpx
from openai import AsyncOpenAI

ACTIVE_RUNTIME = contextvars.ContextVar('model_runtime', default=None)

RESERVE = '''
local stamp=redis.call('TIME'); local now=tonumber(stamp[1])*1000+tonumber(stamp[2])/1000
local policy=ARGV[1]..':'..ARGV[2]..':'..ARGV[3]
local previous=redis.call('HGET',KEYS[3],'policy')
if previous and previous~=policy then return {-2,0} end
redis.call('HSET',KEYS[3],'policy',policy)
local expired=redis.call('ZRANGEBYSCORE',KEYS[1],'-inf',now-60000)
local used=tonumber(redis.call('HGET',KEYS[3],'used') or '0')
for _,id in ipairs(expired) do used=used-tonumber(redis.call('HGET',KEYS[2],id) or '0');redis.call('HDEL',KEYS[2],id) end
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now-60000)
redis.call('ZREMRANGEBYSCORE',KEYS[4],'-inf',now)
used=math.max(0,used);redis.call('HSET',KEYS[3],'used',used)
if tonumber(ARGV[4])>tonumber(ARGV[2]) then return {-1,0} end
if redis.call('ZCARD',KEYS[1])>=tonumber(ARGV[1]) or used+tonumber(ARGV[4])>tonumber(ARGV[2]) or redis.call('ZCARD',KEYS[4])>=tonumber(ARGV[3]) then return {0,100} end
redis.call('ZADD',KEYS[1],now,ARGV[5]);redis.call('HSET',KEYS[2],ARGV[5],ARGV[4])
redis.call('HSET',KEYS[3],'used',used+tonumber(ARGV[4]))
redis.call('ZADD',KEYS[4],now+tonumber(ARGV[6]),ARGV[5])
for _,k in ipairs(KEYS) do redis.call('PEXPIRE',k,math.max(180000,tonumber(ARGV[6])+60000)) end
return {1,0}
'''

RELEASE = '''
redis.call('ZREM',KEYS[4],ARGV[1])
local reserved=redis.call('HGET',KEYS[2],ARGV[1])
if reserved and tonumber(ARGV[2])>=0 then
 local actual=tonumber(ARGV[2]);local used=tonumber(redis.call('HGET',KEYS[3],'used') or '0')
 redis.call('HSET',KEYS[2],ARGV[1],actual)
 redis.call('HSET',KEYS[3],'used',math.max(0,used-tonumber(reserved)+actual))
end
return 1
'''


class Quota:
    def __init__(self, redis, settings):
        self.redis, self.settings = redis, settings

    def keys(self, model):
        scope = f'{self.settings.quota_scope}:{self.settings.state_schema}:{self.settings.base_url}:{model}'
        tag = hashlib.sha256(scope.encode()).hexdigest()[:32]
        return [f'agent:quota:{{{tag}}}:{suffix}' for suffix in ('requests','costs','state','active')]

    async def reserve(self, model, tokens):
        identifier = str(uuid.uuid4())
        end = asyncio.get_running_loop().time() + self.settings.model_quota_wait
        while True:
            result = await self.redis.eval(RESERVE, 4, *self.keys(model), self.settings.model_rpm,
                                           self.settings.model_tpm, self.settings.model_concurrency, tokens, identifier,
                                           int((self.settings.model_call_timeout+30)*1000))
            if result[0] == 1:
                return identifier
            if result[0] < 0 or asyncio.get_running_loop().time() >= end:
                return None
            await asyncio.sleep(.1)

    async def release(self, model, identifier, actual=None):
        await self.redis.eval(RELEASE, 4, *self.keys(model), identifier, actual if actual is not None else -1)


class QuotaTransport(httpx.AsyncBaseTransport):
    def __init__(self, inner, quota):
        self.inner, self.quota = inner, quota

    async def handle_async_request(self, request):
        body = json.loads(await request.aread())
        model = body.get('model','unknown')
        # UTF-8 bytes plus protocol overhead conservatively reserve input tokens.
        estimate = len(json.dumps(body,ensure_ascii=False).encode()) + 128
        if 'messages' in body:
            output = body.get('max_completion_tokens')
            if not isinstance(output, int) or output <= 0:
                return httpx.Response(400,json={'error':{'message':'A bounded model output is required','type':'invalid_request_error'}},request=request)
            estimate += output + 64 * len(body['messages'])
        identifier = await self.quota.reserve(model, estimate)
        if not identifier:
            return httpx.Response(429,headers={'retry-after':'1'},json={'error':{'message':'Agent global model quota is busy','type':'rate_limit_error','code':'agent_quota'}},request=request)
        actual = None
        try:
            try:
                async with asyncio.timeout(self.quota.settings.model_call_timeout):
                    response = await self.inner.handle_async_request(request)
                    await response.aread()
            except TimeoutError as exc:
                raise httpx.ReadTimeout('Model request exceeded its total time budget',request=request) from exc
            if response.is_success:
                try:
                    usage = response.json().get('usage') or {}
                    total = usage.get('total_tokens')
                    if type(total) is int and total >= 0:
                        actual = total
                except (ValueError,AttributeError):
                    pass
            return response
        finally:
            try:
                await asyncio.shield(self.quota.release(model,identifier,actual))
            except Exception:
                # A lost response keeps the full reservation; inflight leases expire.
                pass

    async def aclose(self):
        await self.inner.aclose()


class ModelRuntime:
    def __init__(self, settings, redis, transport=None):
        self.settings, self.original_transport = settings, transport
        self.quota = Quota(redis,settings)
        self.http = httpx.AsyncClient(timeout=httpx.Timeout(35,connect=8),trust_env=False,
            transport=QuotaTransport(transport or httpx.AsyncHTTPTransport(retries=0,
                limits=httpx.Limits(max_connections=settings.model_concurrency,max_keepalive_connections=settings.model_concurrency)),self.quota))
        self.client = AsyncOpenAI(api_key=settings.api_key or 'unconfigured',base_url=settings.base_url,max_retries=0,http_client=self.http)

    @contextmanager
    def activate(self):
        token = ACTIVE_RUNTIME.set(self)
        try:
            yield
        finally:
            ACTIVE_RUNTIME.reset(token)

    async def close(self):
        await self.client.close()


def output_limit():
    runtime = ACTIVE_RUNTIME.get()
    return runtime.settings.model_output_tokens if runtime else 2048


class RuntimeMiddleware:
    """Pure ASGI: retain cancellation propagation and per-request ContextVars."""
    def __init__(self, app):
        self.app=app

    async def __call__(self,scope,receive,send):
        owner=scope.get('app')
        runtime=getattr(owner.state,'model_runtime',None) if owner else None
        if runtime is None:
            return await self.app(scope,receive,send)
        with runtime.activate():
            await self.app(scope,receive,send)


@asynccontextmanager
async def model_http(settings, transport=None):
    runtime=ACTIVE_RUNTIME.get()
    if runtime and runtime.settings.base_url==settings.base_url and transport is runtime.original_transport:
        yield runtime.http
        return
    if runtime:
        transport=QuotaTransport(transport or httpx.AsyncHTTPTransport(retries=0),runtime.quota)
    async with httpx.AsyncClient(timeout=12,trust_env=False,transport=transport) as client:
        yield client
