"""PostgreSQL model ledger: reserve before sending, persist actual usage afterwards."""
import asyncio
import contextvars
import hashlib
import json
import time

import httpx
from memory.task_store import TaskStore, DB_NOW

INVESTIGATION = contextvars.ContextVar('ops_investigation', default=None)


class UsageTransport(httpx.AsyncBaseTransport):
    def __init__(self, store, inner=None):
        self.owns_store = not isinstance(store, TaskStore)
        self.store = TaskStore(store) if self.owns_store else store
        self.inner = inner or httpx.AsyncHTTPTransport(retries=0)

    async def handle_async_request(self, request):
        state = INVESTIGATION.get()
        if state is None:
            raise RuntimeError('Model requests require an investigation context')
        if state['requests'] >= 8:
            raise RuntimeError('Eight actual model requests per investigation exceeded')
        body = json.loads(request.content)
        endpoint = request.url.path.rstrip('/').rsplit('/', 1)[-1]
        kind = {'completions': 'chat', 'embeddings': 'embedding'}.get(endpoint, 'rerank')
        if kind == 'chat':
            body['max_completion_tokens'] = 2400
        digest = hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()
        def reserve(conn):
            conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))', ('ops-usage:' + state['task_id'],))
            count = conn.execute('SELECT COUNT(*) AS n FROM ops_model_usage WHERE task_id=%s', (state['task_id'],)).fetchone()['n']
            if count >= 8:
                raise RuntimeError('Eight persistent model reservations per investigation exceeded')
            return conn.execute(f"INSERT INTO ops_model_usage(task_id,started,model,request_sha256,kind,error) "
                f"VALUES(%s,{DB_NOW},%s,%s,%s,'REQUEST_STARTED_USAGE_UNKNOWN') RETURNING id",
                (state['task_id'], body.get('model'), digest, kind)).fetchone()['id']
        identifier = await self.store.call(reserve)
        state['requests'] += 1
        begin, status, error, payload = time.perf_counter(), None, None, {}
        forwarded = httpx.Request(request.method, request.url,
            headers={k: v for k, v in request.headers.items() if k.lower() != 'content-length'},
            json=body, extensions=request.extensions)
        try:
            response = await self.inner.handle_async_request(forwarded)
            raw = await response.aread()
            status = response.status_code
            if status == 200:
                payload = json.loads(raw)
            else:
                error = 'HTTP_' + str(status)
            return response
        except BaseException as exc:
            error = type(exc).__name__
            raise
        finally:
            usage = payload.get('usage') or {}
            public = [{'finish_reason': c.get('finish_reason'), 'content': c.get('message', {}).get('content'),
                       'tool_calls': c.get('message', {}).get('tool_calls')} for c in payload.get('choices', [])]
            await self.store.call(lambda conn: conn.execute('''UPDATE ops_model_usage SET duration_ms=%s,status=%s,
                input_tokens=%s,output_tokens=%s,total_tokens=%s,usage=%s,error=%s,response=%s WHERE id=%s''',
                ((time.perf_counter() - begin) * 1000, status, usage.get('prompt_tokens', usage.get('input_tokens')),
                 usage.get('completion_tokens', usage.get('output_tokens')), usage.get('total_tokens'),
                 json.dumps(usage), error, json.dumps(public, ensure_ascii=False), identifier)))

    async def aclose(self):
        # The shared lifespan owner closes the transport after all bounded calls.
        pass

    async def close_owned_store(self):
        if self.owns_store:
            await self.store.aclose()


def usage_rows(store, task_id=None):
    owns_store = not isinstance(store, TaskStore)
    journal = TaskStore(store) if owns_store else store
    try:
        return journal.call_sync(lambda c: c.execute('SELECT * FROM ops_model_usage'
            + (' WHERE task_id=%s' if task_id else '') + ' ORDER BY id', (task_id,) if task_id else ()).fetchall())
    finally:
        if owns_store:
            journal.close()
