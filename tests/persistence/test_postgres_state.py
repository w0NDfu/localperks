"""Real PostgreSQL/Redis, synthetic records; model/Java HTTP transports are explicit test doubles."""
import asyncio
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest
from redis.asyncio import Redis

from api.actions import ActionConfirmation, ActionRequest, ActionService
from core.config import Settings
from core.context import ToolContext
from core.state import AgentError
from memory.migrate_sqlite import migrate, snapshot
from memory.redis_memory import RedisMemory
from memory.task_store import TaskStore
from ops.usage import INVESTIGATION, UsageTransport, usage_rows
from orchestration.state import TaskNode, TaskResult
from tools.java_api import JavaAPI


def store():
    return TaskStore(os.environ['AGENT_TEST_STATE_DSN'])


def test_two_processes_compete_for_one_durable_lease():
    async def run():
        journal = store()
        identifier = await journal.create('user:1', 'session', {'message': 'safe'})
        code = '''import asyncio,json,os,sys
from memory.task_store import TaskStore
from core.state import AgentError
async def main():
 s=TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
 try:
  try: await s.claim('user:1',sys.argv[1]);print('CLAIMED',flush=True)
  except AgentError as e: print(e.code,flush=True)
 finally: await s.aclose()
asyncio.run(main())'''
        workers = [subprocess.Popen([sys.executable, '-c', code, identifier], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, encoding='utf-8', env=os.environ.copy()) for _ in range(2)]
        outputs = [await asyncio.to_thread(worker.communicate, timeout=20) for worker in workers]
        assert sorted(out.strip() for out, _ in outputs) == ['CLAIMED', 'TASK_BUSY'], outputs
        assert (await journal.get('user:1', identifier))['revision'] == 1
        with pytest.raises(AgentError) as denied:
            await journal.get('user:2', identifier)
        assert denied.value.status_code == 404
    asyncio.run(run())


def test_checkpoint_survives_reopen_and_stale_worker_cannot_finish():
    async def run():
        first, second = store(), store()
        identifier = await first.create('guest', 'session', {'budget': 80})
        await first.claim('guest', identifier)
        await first.checkpoint(identifier, TaskNode(id='parse', agent='supervisor', operation='parse', volatile=False),
                               TaskResult(node_id='parse', agent='supervisor', status='completed', data={'people': 3}))
        await first.call(lambda c: c.execute('UPDATE agent_tasks SET lease_until=0 WHERE id=%s', (identifier,)))
        await second.claim('guest', identifier)
        assert (await second.checkpoints('guest', identifier))['parse']['data']['people'] == 3
        with pytest.raises(AgentError) as stale:
            await first.finish(identifier, 'completed', {'wrong': True})
        assert stale.value.code == 'TASK_LEASE_LOST'
        await second.finish(identifier, 'completed', {'right': True})
        reopened = store()
        assert (await reopened.get('guest', identifier))['output'] == {'right': True}
        await reopened.event(identifier, {'type': 'audit', 'api_key': 'private', 'message': 'sk-01234567890123456789'})
        assert 'private' not in json.dumps(await reopened.events('guest', identifier))
    asyncio.run(run())


def test_cross_instance_confirmation_sends_exactly_one_business_post():
    async def run():
        writes = []
        def java(request):
            if request.method == 'POST':
                writes.append(request.url.path)
                data = {'orderId': '123', 'statusCode': 10}
            else:
                data = [{'id': 8, 'type': 1, 'stock': 5, 'payValue': 6000, 'actualValue': 10000,
                         'beginTime': '2020-01-01T00:00:00+08:00', 'endTime': '2099-01-01T00:00:00+08:00'}]
            return httpx.Response(200, json={'success': True, 'data': data})
        services = [ActionService(store(), Settings(api_key='')) for _ in range(2)]
        async with httpx.AsyncClient(base_url='http://java.test', transport=httpx.MockTransport(java)) as client:
            ctx = ToolContext(java=JavaAPI(client, 'login'), authenticated=True, scope='user:1')
            prepared = await services[0].prepare(ActionRequest(operation='seckill', voucher_id=8), ctx)
            confirmation = ActionConfirmation(confirmed=True, confirmation_token=prepared['confirmation_token'])
            outcomes = await asyncio.gather(*(service.confirm(prepared['action_id'], confirmation, ctx) for service in services), return_exceptions=True)
            assert len(writes) == 1
            assert any(isinstance(result, dict) and result['state'] == 'completed' for result in outcomes)
            assert all(not isinstance(result, BaseException) or isinstance(result, AgentError) for result in outcomes)
    asyncio.run(run())


def test_postgres_usage_records_known_unknown_and_cross_instance_budget():
    async def run():
        journal = store()
        async def wire(request):
            if json.loads(request.content).get('model') == 'failed':
                raise httpx.ReadTimeout('injected response loss', request=request)
            return httpx.Response(200, json={'usage': {'prompt_tokens': 42, 'completion_tokens': 8, 'total_tokens': 50}, 'choices': []})
        transport = UsageTransport(journal, httpx.MockTransport(wire))
        token = INVESTIGATION.set({'task_id': 'usage-test', 'requests': 0})
        try:
            await transport.handle_async_request(httpx.Request('POST', 'http://model.test/chat/completions', json={'model': 'ok'}))
            with pytest.raises(httpx.ReadTimeout):
                await transport.handle_async_request(httpx.Request('POST', 'http://model.test/chat/completions', json={'model': 'failed'}))
            for _ in range(6):
                await transport.handle_async_request(httpx.Request('POST', 'http://model.test/embeddings', json={'model': 'ok'}))
            # A new process/transport counter cannot bypass the database reservation budget.
            INVESTIGATION.set({'task_id': 'usage-test', 'requests': 0})
            with pytest.raises(RuntimeError, match='persistent'):
                await UsageTransport(store(), httpx.MockTransport(wire)).handle_async_request(
                    httpx.Request('POST', 'http://model.test/chat/completions', json={'model': 'ok'}))
        finally:
            INVESTIGATION.reset(token)
        rows = usage_rows(journal, 'usage-test')
        assert len(rows) == 8 and rows[0]['total_tokens'] == 50
        assert rows[1]['total_tokens'] is None and rows[1]['error'] == 'ReadTimeout'
        assert len([row for row in rows if row['kind'] == 'embedding']) == 6
    asyncio.run(run())


def legacy_database(path):
    with sqlite3.connect(path) as conn:
        conn.executescript('''CREATE TABLE agent_tasks(id TEXT PRIMARY KEY,scope TEXT,session_id TEXT,kind TEXT,state TEXT,
          request TEXT,output TEXT,lease_until REAL,revision INTEGER,created REAL,updated REAL);
          CREATE TABLE agent_events(seq INTEGER PRIMARY KEY,task_id TEXT,event TEXT,created REAL);
          CREATE TABLE agent_actions(id TEXT PRIMARY KEY,scope TEXT,idem TEXT,operation TEXT,payload TEXT,preview TEXT,
          nonce TEXT,state TEXT,result TEXT,created REAL,updated REAL);
          INSERT INTO agent_tasks VALUES('legacy','user:1','s','chat','running','{}',NULL,9999999999,2,1,1);
          INSERT INTO agent_events VALUES(700,'legacy','{"type":"old"}',1);
          INSERT INTO agent_actions VALUES('ambiguous','user:1','key','seckill','{}','{}','secret','running',NULL,1,1);''')


def test_legacy_import_is_readonly_idempotent_and_sequences_advance(tmp_path):
    source = tmp_path / 'legacy.sqlite3'
    legacy_database(source)
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    journal = store()
    result = migrate(source, journal, acknowledge_writers_stopped=True)
    assert result['inserted'] == 3
    assert migrate(source, journal, acknowledge_writers_stopped=True)['status'] == 'already_imported'
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    assert not Path(str(source) + '-wal').exists() and not Path(str(source) + '-shm').exists()
    async def check():
        assert (await journal.get('user:1', 'legacy'))['state'] == 'interrupted'
        row = await journal.call(lambda c: c.execute("SELECT state FROM agent_actions WHERE id='ambiguous'").fetchone())
        assert row['state'] == 'running'
        await journal.event('legacy', {'type': 'new'})
        assert (await journal.events('user:1', 'legacy'))[-1]['seq'] > 700
    asyncio.run(check())


def test_conflict_rolls_back_whole_import(tmp_path):
    source = tmp_path / 'legacy.sqlite3'
    legacy_database(source)
    journal = store()
    journal.call_sync(lambda c: c.execute("INSERT INTO agent_events(seq,task_id,event,created) VALUES(700,'other','{}',1)"))
    with pytest.raises(ValueError, match='Conflicting'):
        migrate(source, journal, acknowledge_writers_stopped=True)
    assert journal.call_sync(lambda c: c.execute("SELECT COUNT(*) AS n FROM agent_tasks WHERE id='legacy'").fetchone())['n'] == 0


def test_redis_sessions_and_lock_are_shared_between_clients():
    async def run():
        clients = [Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'], decode_responses=True) for _ in range(2)]
        a, b = [RedisMemory(client) for client in clients]
        try:
            async with a.turn('user:1', None) as (identifier, _):
                await a.save('user:1', identifier, [{'budget': 80}])
                assert await b.load('user:1', identifier) == [{'budget': 80}]
                with pytest.raises(AgentError) as busy:
                    async with b.turn('user:1', identifier):
                        pass
                assert busy.value.code == 'SESSION_BUSY'
        finally:
            for client in clients:
                await client.aclose()
    asyncio.run(run())


def test_runtime_rejects_sqlite_without_creating_a_file(tmp_path):
    target = tmp_path / 'must-not-exist.sqlite3'
    with pytest.raises(AgentError) as missing:
        TaskStore(str(target))
    assert missing.value.code == 'STATE_STORE_NOT_CONFIGURED'
    assert not target.exists()


def test_checkpoint_committed_by_a_crashed_process_can_be_resumed():
    async def run():
        journal = store()
        identifier = await journal.create('guest', 'crash-session', {'message': 'recover'})
        code = '''import asyncio,os,sys
from memory.task_store import TaskStore
from orchestration.state import TaskNode,TaskResult
async def main():
 s=TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
 await s.claim('guest',sys.argv[1])
 await s.checkpoint(sys.argv[1],TaskNode(id='saved',agent='supervisor',operation='parse',volatile=False),TaskResult(node_id='saved',agent='supervisor',status='completed',data={'budget':80}))
 print('COMMITTED_BEFORE_CRASH',flush=True)
 os._exit(17)
asyncio.run(main())'''
        result = await asyncio.to_thread(subprocess.run, [sys.executable, '-c', code, identifier],
            capture_output=True, text=True, encoding='utf-8', env=os.environ.copy(), timeout=20)
        assert result.returncode == 17 and 'COMMITTED_BEFORE_CRASH' in result.stdout
        recovered = store()
        assert (await recovered.checkpoints('guest', identifier))['saved']['data']['budget'] == 80
        with pytest.raises(AgentError):
            await recovered.checkpoint(identifier, TaskNode(id='bad', agent='supervisor', operation='parse'),
                                        TaskResult(node_id='bad', agent='supervisor', status='completed', data={}))
        await recovered.call(lambda c: c.execute('UPDATE agent_tasks SET lease_until=0 WHERE id=%s', (identifier,)))
        await recovered.claim('guest', identifier)
        await recovered.finish(identifier, 'completed', {'recovered': True})
        assert (await journal.get('guest', identifier))['output']['recovered'] is True
    asyncio.run(run())


def test_postgres_failure_is_503_and_never_a_local_fallback(tmp_path):
    with pytest.raises(AgentError) as unavailable:
        TaskStore('postgresql://postgres:isolated@127.0.0.1:1/unreachable')
    assert unavailable.value.code == 'STATE_STORE_UNAVAILABLE' and unavailable.value.status_code == 503
    assert not list(tmp_path.glob('*.sqlite*'))


def test_api_status_reports_postgres_readiness_with_real_state_and_redis():
    from main import create_app
    from tests_support import TestMemory
    async def run():
        app = create_app(settings=Settings(api_key='', redis_url=os.environ['AGENT_TEST_REDIS_URL']), memory=TestMemory(), transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={'success': True, 'data': []})))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(base_url='http://test', transport=httpx.ASGITransport(app)) as client:
                response = await client.get('/agent/status')
                assert response.status_code == 200
                data = response.json()
                assert data['workflow_store'] == 'postgresql'
                assert data['state_store'] is True and data['redis'] is True and data['ready'] is True
    asyncio.run(run())
