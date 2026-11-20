"""Prove ordinal visibility order using actual uncommitted PG transactions."""
import asyncio
import json
import os
import threading
import uuid

import pytest
from redis.asyncio import Redis

from api.shared_dispatch import SharedDispatcher
from memory.credentials import CredentialVault
from memory.notifications import EventHub
from memory.task_store import TaskStore


async def wait_for(check, timeout=2):
    deadline = asyncio.get_running_loop().time() + timeout
    while not await check():
        assert asyncio.get_running_loop().time() < deadline, 'expected lock boundary was not observed'
        await asyncio.sleep(.01)


@pytest.mark.parametrize('rollback_first', [False, True], ids=['first_commits', 'first_rolls_back'])
def test_sequence_allocation_cannot_overtake_the_uncommitted_admission(monkeypatch, rollback_first):
    async def run():
        stores = [TaskStore(os.environ['AGENT_TEST_STATE_DSN']) for _ in range(3)]
        clients = [Redis.from_url(os.environ['AGENT_TEST_REDIS_URL'], decode_responses=True) for _ in range(2)]
        dispatchers = []
        async def unused(*args):
            raise AssertionError('this test controls claiming/finishing explicitly')
        for journal, redis in zip(stores, clients):
            vault = CredentialVault(redis, os.environ['AGENT_CREDENTIAL_KEY'], journal.schema)
            dispatchers.append(SharedDispatcher(journal, vault, EventHub(journal), unused))
        a, b, observer = stores
        qa, qb = dispatchers
        # Precreate a committed session so B must wait on the admission lock,
        # rather than on an incidental conflicting INSERT of the session row.
        await a.create('user:1', 'same-session', {'message': 'setup only'})
        allocated, release, second_entered = threading.Event(), threading.Event(), threading.Event()
        inspected = {}
        original_a, original_b = a.admit, b.admit
        request_a, request_b = str(uuid.uuid4()), str(uuid.uuid4())

        async def hold_first(*args, **kwargs):
            enqueue = kwargs['queue_hook']
            def paused(conn, task, session, created):
                enqueue(conn, task, session, created)
                row = conn.execute('SELECT ordinal FROM agent_jobs WHERE task_id=%s', (task,)).fetchone()
                inspected['first_ordinal'] = row['ordinal']
                allocated.set()
                if not release.wait(3):
                    raise AssertionError('first admission barrier was not released')
                if rollback_first:
                    raise RuntimeError('deliberate admission rollback after ordinal allocation')
            return await original_a(*args, **{**kwargs, 'queue_hook': paused})

        async def observe_second(*args, **kwargs):
            enqueue = kwargs['queue_hook']
            def observed(conn, task, session, created):
                inspected['second_pid'] = conn.execute('SELECT pg_backend_pid() AS pid').fetchone()['pid']
                second_entered.set()
                enqueue(conn, task, session, created)
            return await original_b(*args, **{**kwargs, 'queue_hook': observed})

        monkeypatch.setattr(a, 'admit', hold_first)
        monkeypatch.setattr(b, 'admit', observe_second)
        first = asyncio.create_task(qa.submit('user:1', request_a, 'first', {'message': 'T1'}, 'same-session', 'fixture-login'))
        second = None
        try:
            assert await asyncio.to_thread(allocated.wait, 2)
            second = asyncio.create_task(qb.submit('user:1', request_b, 'second', {'message': 'T2'}, 'same-session', 'fixture-login'))
            assert await asyncio.to_thread(second_entered.wait, 2)
            async def blocked_on_advisory():
                state = await observer.call(lambda c: c.execute(
                    'SELECT wait_event_type,wait_event FROM pg_stat_activity WHERE pid=%s',
                    (inspected['second_pid'],)).fetchone())
                return state and state['wait_event_type'] == 'Lock' and state['wait_event'] == 'advisory'
            await wait_for(blocked_on_advisory)
            assert not first.done() and not second.done()
            # A already has an ordinal, yet neither request is committed.
            assert await observer.call(lambda c: c.execute('SELECT COUNT(*) AS n FROM agent_jobs').fetchone()['n']) == 0
            assert await qb.claim_next() is None

            release.set()
            if rollback_first:
                with pytest.raises(RuntimeError, match='deliberate admission rollback'):
                    await first
            else:
                first_receipt, _ = await first
            second_receipt, _ = await second
            rows = await observer.call(lambda c: c.execute('SELECT task_id,ordinal,payload FROM agent_jobs ORDER BY ordinal').fetchall())
            expected = ['T2'] if rollback_first else ['T1', 'T2']
            assert [json.loads(row['payload'])['message'] for row in rows] == expected
            if rollback_first:
                assert rows[0]['ordinal'] > inspected['first_ordinal']
                assert await observer.call(lambda c: c.execute('SELECT task_id FROM agent_requests WHERE request_id=%s', (request_a,)).fetchone()) is None
            else:
                assert rows[0]['ordinal'] == inspected['first_ordinal']
                assert rows[1]['ordinal'] > rows[0]['ordinal']
                first_job = await qb.claim_next()
                assert first_job['task_id'] == first_receipt['id']
                assert await qa.claim_next() is None  # a running queue head blocks T2
                await b.claim('user:1', first_receipt['id'], admitted=True, job_owner=first_job['owner'])
                await b.finish(first_receipt['id'], 'completed', {'answer': 'T1'})
                assert await qa.claim_next() is None  # task commit alone is not queue acknowledgement
                assert await qb._finish(first_job, 'done') == 1
            next_job = await qa.claim_next()
            assert next_job['task_id'] == second_receipt['id']
        finally:
            release.set()
            await asyncio.gather(*(t for t in [first, second] if t is not None), return_exceptions=True)
            for redis in clients:
                await redis.aclose()
    asyncio.run(run())
