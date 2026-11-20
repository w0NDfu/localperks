"""Real PostgreSQL/Redis regression for expired executors and atomic conversations."""
import asyncio
import json
import os
import subprocess
import sys
import threading
import uuid

import httpx
import pytest
from redis.asyncio import Redis
import psycopg

from core.config import Settings
from core.state import AgentError
from main import create_app
from memory.conversation_memory import ConversationMemory, CACHE_PUT
from memory.task_store import TaskStore
from orchestration.state import TaskNode, TaskResult


def store():
    return TaskStore(os.environ['AGENT_TEST_STATE_DSN'])


def turn(answer):
    return {'user': 'question', 'assistant': answer, 'trace': []}


async def expire(s, task):
    await s.call(lambda c: c.execute('UPDATE agent_tasks SET lease_until=0 WHERE id=%s', (task,)))


def test_takeover_rejects_every_stale_write_and_commits_one_turn():
    async def run():
        a, b = store(), store()
        task = await a.create('user:1', 'session', {'message': 'question'})
        await a.claim('user:1', task)
        first = a.execution(task)['epoch']
        await expire(a, task)
        await b.claim('user:1', task)
        assert b.execution(task)['epoch'] == first + 1
        node = TaskNode(id='old', agent='supervisor', operation='parse')
        result = TaskResult(node_id='old', agent='supervisor', status='completed', data={'wrong': True})
        for attempt in [lambda: a.renew(task), lambda: a.event(task, {'type': 'wrong'}),
                        lambda: a.checkpoint(task, node, result),
                        lambda: a.finish(task, 'completed', {'answer': 'old'}, turn=turn('old'))]:
            with pytest.raises(AgentError) as error:
                await attempt()
            assert error.value.code == 'TASK_LEASE_LOST'
        await b.finish(task, 'completed', {'answer': 'new'}, turn=turn('new'))
        assert (await b.conversation_snapshot('user:1', 'session'))['turns'] == [{**turn('new'), 'task_id': task}]
        assert (await b.get('user:1', task))['output'] == {'answer': 'new'}
        assert await b.call(lambda c: c.execute('SELECT COUNT(*) AS n FROM agent_turns').fetchone()['n']) == 1
        with pytest.raises(AgentError):
            await a.finish(task, 'completed', {'answer': 'old'}, turn=turn('old'))
    asyncio.run(run())


def test_expired_lease_cannot_renew_or_commit_before_a_takeover():
    async def run():
        s = store()
        task = await s.create('user:1', 'session', {'message': 'q'})
        await s.claim('user:1', task)
        await expire(s, task)
        for attempt in [lambda: s.renew(task), lambda: s.finish(task, 'completed', {'answer': 'late'}, turn=turn('late'))]:
            with pytest.raises(AgentError, match='租约'):
                await attempt()
        assert not (await s.conversation_snapshot('user:1', 'session'))['turns']
    asyncio.run(run())


def test_result_history_event_and_outbox_rollback_together():
    async def run():
        s = store()
        task = await s.create('user:1', 'session', {'message': 'q'})
        await s.claim('user:1', task)
        def inject(c):
            c.execute("""CREATE FUNCTION reject_completion() RETURNS trigger AS $$ BEGIN
                IF NEW.event::jsonb->>'state'='completed' THEN RAISE EXCEPTION 'injected commit failure'; END IF;
                RETURN NEW; END; $$ LANGUAGE plpgsql""")
            c.execute('CREATE TRIGGER reject_completion BEFORE INSERT ON agent_events FOR EACH ROW EXECUTE FUNCTION reject_completion()')
        await s.call(inject)
        with pytest.raises(AgentError):
            await s.finish(task, 'completed', {'answer': 'answer'}, turn=turn('answer'))
        assert (await s.get('user:1', task))['state'] == 'running'
        assert (await s.conversation_snapshot('user:1', 'session')) == {'revision': 0, 'turns': []}
        assert await s.call(lambda c: c.execute('SELECT COUNT(*) AS n FROM agent_session_cache_outbox').fetchone()['n']) == 0
        await s.call(lambda c: c.execute('DROP TRIGGER reject_completion ON agent_events'))
        await s.finish(task, 'completed', {'answer': 'answer'}, turn=turn('answer'))
        assert (await s.conversation_snapshot('user:1', 'session'))['revision'] == 1
    asyncio.run(run())


def test_same_session_is_serialized_in_pg_and_next_claim_gets_committed_history():
    async def run():
        a, b = store(), store()
        one = await a.create('user:1', 'session', {'message': 'one'})
        two = await b.create('user:1', 'session', {'message': 'two'})
        await a.claim('user:1', one)
        with pytest.raises(AgentError) as busy:
            await b.claim('user:1', two)
        assert busy.value.code == 'SESSION_BUSY'
        await a.finish(one, 'completed', {'answer': 'first'}, turn=turn('first'))
        history = await b.claim('user:1', two)
        assert history[0]['assistant'] == 'first'
        await b.finish(two, 'completed', {'answer': 'second'}, turn=turn('second'))
        assert (await b.conversation_snapshot('user:1', 'session'))['revision'] == 2
        with pytest.raises(AgentError):
            await a.conversation_snapshot('user:2', 'session')
    asyncio.run(run())


def test_heartbeat_extends_a_live_lease_and_stops_after_context_exit():
    async def run():
        s = store()
        task = await s.create('user:1', 'session', {'message': 'q'})
        await s.claim('user:1', task, lease_seconds=.3)
        async with s.keep_lease(task):
            await asyncio.sleep(.65)
            await s.event(task, {'type': 'still_owned'})
        await s.finish(task, 'completed', {'answer': 'ok'}, turn=turn('ok'))
        assert (await s.get('user:1', task))['state'] == 'completed'
    asyncio.run(run())


def test_lost_heartbeat_cancels_inference_without_a_conversation_write():
    async def run():
        a, b = store(), store()
        task = await a.create('user:1', 'session', {'message': 'q'})
        await a.claim('user:1', task, lease_seconds=.3)
        with pytest.raises(AgentError) as lost:
            async with a.keep_lease(task):
                await expire(b, task)
                await b.claim('user:1', task)
                await asyncio.sleep(1)
        assert lost.value.code == 'TASK_LEASE_LOST'
        assert not (await b.conversation_snapshot('user:1', 'session'))['turns']
    asyncio.run(run())


def test_cache_failure_retries_after_restart_and_old_versions_cannot_overwrite():
    async def run():
        s = store()
        r = Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'], decode_responses=True)
        memory = ConversationMemory(s, r)
        task = await s.create('user:1', 'session', {'message': 'q'})
        await s.claim('user:1', task)
        await s.finish(task, 'completed', {'answer': 'committed'}, turn=turn('committed'))
        jobs = await s.claim_cache_updates()
        assert len(jobs) == 1
        await s.retry_cache(jobs[0])
        assert (await s.get('user:1', task))['state'] == 'completed'
        reopened = store()
        await reopened.call(lambda c: c.execute('UPDATE agent_session_cache_outbox SET next_attempt=0'))
        repaired = ConversationMemory(reopened, r)
        await repaired.flush()
        cache = await r.hgetall(memory.key('user:1', 'session'))
        assert cache['revision'] == '1' and json.loads(cache['data'])[0]['assistant'] == 'committed'
        assert await r.eval(CACHE_PUT, 1, memory.key('user:1', 'session'), '0', '[]') == 0
        assert await r.hget(memory.key('user:1', 'session'), 'data') == cache['data']
        await r.delete(memory.key('user:1', 'session'))
        assert (await repaired.load('user:1', 'session'))[0]['assistant'] == 'committed'
        await r.aclose()
    asyncio.run(run())


def test_http_answer_succeeds_when_cache_write_fails_and_history_survives_restart(monkeypatch):
    async def run():
        async def broken_cache(_self, _job):
            raise ConnectionError('isolated cache write failure')
        monkeypatch.setattr(ConversationMemory, 'materialize', broken_cache)
        def java(_request):
            return httpx.Response(200, json={'success': True, 'data': {'id': 4, 'name': '店铺4'}})
        settings = Settings(api_key='', state_dsn=os.environ['AGENT_TEST_STATE_DSN'])
        app = create_app(settings=settings, transport=httpx.MockTransport(java))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://fixture') as client:
                response = await client.post('/agent/chat', json={'message': '查询店铺4详情'})
                assert response.status_code == 200, response.text
                value = response.json()
                assert (await app.state.task_store.get('guest', value['task_id']))['state'] == 'completed'
        second = create_app(settings=settings, transport=httpx.MockTransport(java))
        async with second.router.lifespan_context(second):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(second), base_url='http://fixture') as client:
                history = await client.get('/agent/conversations/' + value['session_id'])
                assert history.status_code == 200 and len(history.json()['turns']) == 1
                result = await client.post('/agent/chat', json={'message': '查询店铺4详情', 'session_id': value['session_id']})
                assert result.status_code == 200, result.text
        assert (await store().conversation_snapshot('guest', value['session_id']))['revision'] == 2
    asyncio.run(run())


def test_late_model_response_cannot_touch_history_after_another_app_finishes(monkeypatch):
    async def run():
        from orchestration.supervisor import Supervisor
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []
        async def inference(_self, *args, **kwargs):
            calls.append('model')
            if len(calls) == 1:
                entered.set()
                await release.wait()
                return {'answer': 'old executor answer', 'state': 'completed'}
            return {'answer': 'new executor answer', 'state': 'completed'}
        monkeypatch.setattr(Supervisor, 'run', inference)
        settings = Settings(api_key='', state_dsn=os.environ['AGENT_TEST_STATE_DSN'])
        apps = [create_app(settings=settings) for _ in range(2)]
        async with apps[0].router.lifespan_context(apps[0]), apps[1].router.lifespan_context(apps[1]):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(apps[0]), base_url='http://first') as a, \
                       httpx.AsyncClient(transport=httpx.ASGITransport(apps[1]), base_url='http://second') as b:
                old = asyncio.create_task(a.post('/agent/chat', json={'message': 'hello'}))
                try:
                    await asyncio.wait_for(entered.wait(), 3)
                    journal = apps[1].state.task_store
                    task = await journal.call(lambda c: c.execute('SELECT id,session_id FROM agent_tasks').fetchone())
                    await expire(journal, task['id'])
                    recovered = await b.post('/agent/tasks/' + task['id'] + '/resume', json={'message': '继续'})
                    assert recovered.status_code == 200, recovered.text
                    release.set()
                    rejected = await old
                    assert rejected.status_code == 409 and rejected.json()['detail']['code'] == 'TASK_LEASE_LOST'
                    snapshot = await journal.conversation_snapshot('guest', task['session_id'])
                    assert len(snapshot['turns']) == 1 and snapshot['revision'] == 1
                    assert snapshot['turns'][0]['assistant'] == 'new executor answer'
                    assert (await journal.get('guest', task['id']))['output']['answer'] == 'new executor answer'
                    await journal.call(lambda c: c.execute('UPDATE agent_session_cache_outbox SET next_attempt=0,lease_until=0'))
                    await apps[1].state.memory.flush()
                    cached = await apps[1].state.redis.hget(apps[1].state.memory.key('guest', task['session_id']), 'data')
                    assert 'new executor answer' in cached and 'old executor answer' not in cached
                finally:
                    release.set()
                    await asyncio.gather(old, return_exceptions=True)
    asyncio.run(run())


def test_cache_ack_does_not_remove_a_newer_revision_and_missing_cache_reads_current_db():
    async def run():
        s = store()
        redis = Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'], decode_responses=True)
        memory = ConversationMemory(s, redis)
        first = await s.create('user:1', 'session', {'message': 'first'})
        await s.claim('user:1', first)
        await s.finish(first, 'completed', {'answer': 'first'}, turn=turn('first'))
        old = (await s.claim_cache_updates())[0]
        second = await s.create('user:1', 'session', {'message': 'second'})
        await s.claim('user:1', second)
        await s.finish(second, 'completed', {'answer': 'second'}, turn=turn('second'))
        await s.acknowledge_cache(old, 1)
        assert await s.call(lambda c: c.execute('SELECT revision FROM agent_session_cache_outbox').fetchone()['revision']) == 2
        await memory.flush()
        assert await redis.hget(memory.key('user:1', 'session'), 'revision') == '2'
        data = json.loads(await redis.hget(memory.key('user:1', 'session'), 'data'))
        assert [t['assistant'] for t in data] == ['first', 'second']
        await redis.aclose()
    asyncio.run(run())


def test_v2_migration_backfills_only_committed_answers_and_is_repeatable():
    async def run():
        s = store()
        task = await s.create('user:1', 'legacy', {'message': 'legacy question'})
        await s.claim('user:1', task)
        await s.finish(task, 'completed', {'answer': 'legacy committed answer', 'constraints': {'people': 2}})
        def legacy(c):
            c.execute('DROP TABLE agent_session_cache_outbox,agent_turns,agent_conversations')
            c.execute('ALTER TABLE agent_tasks DROP COLUMN execution_epoch')
            c.execute('DELETE FROM agent_schema_version WHERE version=3')
        await s.call(legacy)
        upgraded = store()
        snapshot = await upgraded.conversation_snapshot('user:1', 'legacy')
        assert snapshot['revision'] == 1 and snapshot['turns'][0]['assistant'] == 'legacy committed answer'
        assert snapshot['turns'][0]['constraints'] == {'people': 2}
        again = store()
        assert (await again.conversation_snapshot('user:1', 'legacy')) == snapshot
    asyncio.run(run())


def test_a_stale_os_process_cannot_commit_after_another_process_takes_over():
    async def run():
        journal = store()
        task = await journal.create('user:1', 'process-session', {'message': 'q'})
        code = '''import asyncio,sys
from memory.task_store import TaskStore
from core.state import AgentError
import os
async def run():
 s=TaskStore(os.environ['AGENT_TEST_STATE_DSN'])
 await s.claim('user:1',sys.argv[1])
 print('CLAIMED',flush=True)
 await asyncio.to_thread(sys.stdin.readline)
 try:
  await s.finish(sys.argv[1],'completed',{'answer':'stale' },turn={'user':'q','assistant':'stale'})
 except AgentError as e:
  print(e.code,flush=True)
 else:
  raise AssertionError('stale process committed')
 await s.aclose()
asyncio.run(run())'''
        process = subprocess.Popen([sys.executable, '-u', '-c', code, task], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', env=os.environ.copy())
        try:
            ready = await asyncio.wait_for(asyncio.to_thread(process.stdout.readline), 10)
            assert ready.strip() == 'CLAIMED'
            await expire(journal, task)
            await journal.claim('user:1', task)
            await journal.finish(task, 'completed', {'answer': 'winner'}, turn=turn('winner'))
            output, errors = await asyncio.to_thread(process.communicate, '\n', timeout=10)
            assert process.returncode == 0, errors
            assert 'TASK_LEASE_LOST' in output
            assert (await journal.conversation_snapshot('user:1', 'process-session'))['turns'][0]['assistant'] == 'winner'
        finally:
            if process.poll() is None:
                process.kill()
                await asyncio.to_thread(process.wait)
    asyncio.run(run())


def test_recovered_action_keeps_its_business_key_and_confirmation_does_not_repeat_post():
    async def run():
        from support import BusinessFixture
        business = BusinessFixture()
        app = create_app(settings=Settings(api_key='', state_dsn=os.environ['AGENT_TEST_STATE_DSN']),
                         transport=httpx.MockTransport(business))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://fixture') as client:
                headers = {'authorization': 'user-one'}
                initial = await client.post('/agent/chat', headers=headers, json={'message': '帮我抢券8'})
                assert initial.status_code == 200, initial.text
                value = initial.json()
                resumed = await client.post('/agent/tasks/' + value['task_id'] + '/resume', headers=headers, json={'message': '继续'})
                assert resumed.status_code == 200, resumed.text
                assert resumed.json()['action']['action_id'] == value['action']['action_id']
                assert await app.state.task_store.call(lambda c: c.execute('SELECT COUNT(*) AS n FROM agent_actions').fetchone()['n']) == 1
                snapshot = await app.state.task_store.conversation_snapshot('user:1', value['session_id'])
                assert len(snapshot['turns']) == 1 and snapshot['turns'][0]['user'] == '帮我抢券8'
                action = value['action']
                for _ in range(2):
                    confirmed = await client.post('/agent/actions/' + action['action_id'] + '/confirm', headers=headers,
                        json={'confirmed': True, 'confirmation_token': action['confirmation_token']})
                    assert confirmed.status_code == 200, confirmed.text
                assert business.writes == 1
    asyncio.run(run())


def test_retrying_a_completed_task_returns_its_result_without_another_inference(monkeypatch):
    async def run():
        from orchestration.supervisor import Supervisor
        calls = []
        async def inference(*args, **kwargs):
            calls.append(1)
            return {'answer': 'committed answer', 'state': 'completed'}
        monkeypatch.setattr(Supervisor, 'run', inference)
        app = create_app(settings=Settings(api_key='', state_dsn=os.environ['AGENT_TEST_STATE_DSN']))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://fixture') as client:
                result = await client.post('/agent/chat', json={'message': 'hello'})
                value = result.json()
                resumed = await client.post('/agent/tasks/' + value['task_id'] + '/resume', json={'message': '重试'})
                assert resumed.status_code == 200 and resumed.json()['answer'] == 'committed answer'
                assert len(calls) == 1
                assert (await app.state.task_store.conversation_snapshot('guest', value['session_id']))['revision'] == 1
    asyncio.run(run())


def test_display_rejects_a_stale_cache_and_uses_only_the_owned_committed_revision():
    async def run():
        s = store()
        r = Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'], decode_responses=True)
        memory = ConversationMemory(s, r)
        task = await s.create('user:1', 'session', {'message': 'q'})
        await s.claim('user:1', task)
        await s.finish(task, 'completed', {'answer': 'valid'}, turn=turn('valid'))
        await r.hset(memory.key('user:1', 'session'), mapping={'revision': '0', 'data': '[{"assistant":"stale"}]'})
        assert (await memory.load('user:1', 'session'))[0]['assistant'] == 'valid'
        with pytest.raises(AgentError):
            await memory.load('user:2', 'session')
        await memory.flush()
        assert (await memory.load('user:1', 'session'))[0]['assistant'] == 'valid'
        # Missing/malformed cache is rebuildable, not a failed conversation.
        await r.hset(memory.key('user:1', 'session'), 'data', 'broken json')
        assert (await memory.load('user:1', 'session'))[0]['assistant'] == 'valid'
        await r.aclose()
    asyncio.run(run())


def test_result_update_holds_the_row_until_the_entire_commit_finishes(monkeypatch):
    async def run():
        a, b = store(), store()
        task = await a.create('user:1', 'locked-session', {'message': 'q'})
        await a.claim('user:1', task)
        entered, release = threading.Event(), threading.Event()
        original = a._conversation
        def pause_after_result_update(conn, *args, **kwargs):
            entered.set()
            assert release.wait(2), 'test did not release the commit barrier'
            return original(conn, *args, **kwargs)
        monkeypatch.setattr(a, '_conversation', pause_after_result_update)
        commit = asyncio.create_task(a.finish(task, 'completed', {'answer': 'winner'}, turn=turn('winner')))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            def contend(conn):
                conn.execute("SET LOCAL lock_timeout='100ms'")
                conn.execute('UPDATE agent_tasks SET execution_epoch=execution_epoch+1 WHERE id=%s', (task,))
            with pytest.raises(AgentError) as blocked:
                await b.call(contend)
            assert isinstance(blocked.value.__cause__, psycopg.errors.LockNotAvailable)
            assert not commit.done()
        finally:
            release.set()
            await commit
        assert (await b.get('user:1', task))['output'] == {'answer': 'winner'}
        with pytest.raises(AgentError) as terminal:
            await b.claim('user:1', task, admitted=True)
        assert terminal.value.code == 'TASK_BUSY'
        assert (await b.conversation_snapshot('user:1', 'locked-session'))['revision'] == 1
    asyncio.run(run())


def test_idle_transaction_timeout_rolls_back_the_owner_and_releases_the_row():
    async def run():
        a, b = store(), store()
        task = await a.create('user:1', 'idle-session', {'message': 'q'})
        await a.claim('user:1', task)
        configured = await a.call(lambda c: c.execute('SHOW idle_in_transaction_session_timeout').fetchone())
        assert list(configured.values()) == ['5s']
        with pytest.raises(psycopg.Error):
            with a.connect() as connection:
                connection.execute("SET LOCAL idle_in_transaction_session_timeout='150ms'")
                connection.execute("UPDATE agent_tasks SET output='uncommitted' WHERE id=%s", (task,))
                # Deliberate client pause after taking the lock, not pg_sleep:
                # this specifically tests idle-in-transaction termination.
                await asyncio.sleep(.35)
                connection.execute('SELECT 1')
        assert (await b.get('user:1', task))['output'] is None
        await expire(b, task)
        await b.claim('user:1', task)
        await b.finish(task, 'completed', {'answer': 'recovered'}, turn=turn('recovered'))
        assert (await b.conversation_snapshot('user:1', 'idle-session'))['turns'][0]['assistant'] == 'recovered'
    asyncio.run(run())
