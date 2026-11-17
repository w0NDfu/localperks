"""PostgreSQL session FIFO, global queue/worker limits and fenced recovery."""
import asyncio
import json
import uuid

from core.state import AgentError
from memory.task_store import DB_NOW, dump


class SharedDispatcher:
    def __init__(self, store, vault, hub, execute, *, workers=16, max_pending=10000,
                 per_user=8, global_workers=256, max_wait=300, lease_seconds=150):
        self.store, self.vault, self.hub, self.execute = store, vault, hub, execute
        self.workers, self.max_pending, self.per_user = workers, max_pending, per_user
        self.global_workers, self.max_wait, self.lease_seconds = global_workers, max_wait, lease_seconds
        self.tasks = {}
        self.stopping = False
        self.runners = []

    async def start(self):
        self.runners = [asyncio.create_task(self._worker()) for _ in range(self.workers)]

    async def submit(self, scope, request_id, fingerprint, payload, session_id, authorization):
        reference = await self.vault.put(scope, authorization)
        retained = False
        def enqueue(conn, task_id, session, created):
            nonlocal retained
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('agent-shared-admission',0))")
            task=conn.execute(f'SELECT state,lease_until,{DB_NOW} AS server_now FROM agent_tasks WHERE id=%s',(task_id,)).fetchone()
            task_state=task['state']
            if task_state=='running' and task['lease_until']>task['server_now']:
                return
            if not created and task_state not in ('accepted','running','waiting_auth','interrupted'):
                return
            existing = conn.execute('SELECT * FROM agent_jobs WHERE task_id=%s FOR UPDATE', (task_id,)).fetchone()
            if existing and existing['state'] not in ('reauth_required', 'expired'):
                return
            count = conn.execute("SELECT COUNT(*) AS n FROM agent_jobs WHERE state IN ('queued','running','reauth_required')").fetchone()['n']
            owned = conn.execute("SELECT COUNT(*) AS n FROM agent_jobs WHERE scope=%s AND state IN ('queued','running','reauth_required')", (scope,)).fetchone()['n']
            already_counted=bool(existing and existing['state']=='reauth_required')
            if count - already_counted >= self.max_pending or owned - already_counted >= self.per_user:
                raise AgentError('SERVICE_BUSY', '客服任务队列已满，请稍后重试', 503)
            if existing:
                conn.execute(f"UPDATE agent_jobs SET state='queued',credential_ref=%s,deadline={DB_NOW}+%s,owner=NULL,lease_until=0 WHERE task_id=%s",
                             (reference, self.max_wait, task_id))
                conn.execute("UPDATE agent_tasks SET state='accepted',lease_owner=NULL,lease_until=0 WHERE id=%s", (task_id,))
            else:
                if not created:
                    conn.execute("UPDATE agent_tasks SET state='accepted',lease_owner=NULL,lease_until=0 WHERE id=%s",(task_id,))
                conn.execute(f"INSERT INTO agent_jobs(task_id,scope,session_id,payload,credential_ref,state,deadline) VALUES(%s,%s,%s,%s,%s,'queued',{DB_NOW}+%s)",
                             (task_id, scope, session, dump(payload), reference, self.max_wait))
            retained = True
        try:
            return await self.store.admit(scope, request_id, fingerprint, payload, session_id, queue_hook=enqueue)
        finally:
            if not retained:
                await self.vault.delete(reference)

    async def claim_next(self):
        owner = str(uuid.uuid4())
        def claim(conn):
            # Serialize a short capacity decision, never hold a connection during inference.
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('agent-shared-claim',0))")
            stale = conn.execute(f"""WITH batch AS (SELECT task_id FROM agent_jobs WHERE state IN ('queued','reauth_required') AND deadline<{DB_NOW}
                ORDER BY ordinal FOR UPDATE SKIP LOCKED LIMIT 64)
                UPDATE agent_jobs SET state='expired',owner=NULL,lease_until=0 FROM batch WHERE agent_jobs.task_id=batch.task_id RETURNING agent_jobs.task_id""").fetchall()
            for item in stale:
                conn.execute(f"UPDATE agent_tasks SET state='interrupted',updated={DB_NOW},output=%s WHERE id=%s AND state='accepted'",
                             (dump({'error': {'code': 'QUEUE_EXPIRED', 'message': '排队已超时，请重交原请求编号'}}), item['task_id']))
                conn.execute(f"INSERT INTO agent_events(task_id,event,created) VALUES(%s,%s,{DB_NOW})", (item['task_id'], dump({'type':'workflow_state','state':'interrupted'})))
            active = conn.execute(f"SELECT COUNT(*) AS n FROM agent_jobs WHERE state='running' AND lease_until>{DB_NOW}").fetchone()['n']
            if active >= self.global_workers:
                return None
            row = conn.execute(f"""SELECT j.* FROM agent_jobs j
                WHERE ((j.state='queued' AND j.deadline>{DB_NOW}) OR (j.state='running' AND j.lease_until<{DB_NOW}))
                AND NOT EXISTS(SELECT 1 FROM agent_tasks active_task WHERE active_task.id=j.task_id
                    AND active_task.state='running' AND active_task.lease_until>{DB_NOW})
                AND NOT EXISTS(SELECT 1 FROM agent_jobs earlier WHERE earlier.scope=j.scope
                    AND earlier.session_id=j.session_id AND earlier.ordinal<j.ordinal
                    AND earlier.state IN ('queued','running','reauth_required'))
                ORDER BY j.ordinal FOR UPDATE OF j SKIP LOCKED LIMIT 1""").fetchone()
            if row:
                conn.execute(f"UPDATE agent_jobs SET state='running',owner=%s,lease_until={DB_NOW}+%s,attempts=attempts+1 WHERE task_id=%s", (owner, self.lease_seconds, row['task_id']))
                row['owner'] = owner
            return row
        return await self.store.call(claim)

    async def _finish(self, job, state):
        def finish(c):
            changed=c.execute('UPDATE agent_jobs SET state=%s,owner=NULL,lease_until=0 WHERE task_id=%s AND owner=%s',
                              (state,job['task_id'],job['owner'])).rowcount
            if changed and state=='failed':
                c.execute("UPDATE agent_tasks SET state='failed',output=%s WHERE id=%s AND state='accepted'",
                          (dump({'error':{'code':'WORKFLOW_FAILED','message':'任务未能执行，请稍后重试'}}),job['task_id']))
            return changed
        return await self.store.call(finish)

    async def _reauth(self,job,message):
        def mark(c):
            changed=c.execute("UPDATE agent_jobs SET state='reauth_required',owner=NULL,lease_until=0 WHERE task_id=%s AND owner=%s",(job['task_id'],job['owner'])).rowcount
            if changed:
                c.execute(f"UPDATE agent_tasks SET state='waiting_auth',lease_owner=NULL,lease_until=0,output=%s,updated={DB_NOW} WHERE id=%s AND (state='accepted' OR lease_until<{DB_NOW})",
                          (dump({'error':{'code':'REAUTH_REQUIRED','message':message}}),job['task_id']))
                c.execute(f"INSERT INTO agent_events(task_id,event,created) VALUES(%s,%s,{DB_NOW})",(job['task_id'],dump({'type':'reauth_required','code':'REAUTH_REQUIRED'})))
            return changed
        return await self.store.call(mark)

    async def _release_cancelled(self,job):
        def release(c):
            changed=c.execute("UPDATE agent_jobs SET state='queued',owner=NULL,lease_until=0 WHERE task_id=%s AND owner=%s",(job['task_id'],job['owner'])).rowcount
            if changed:
                c.execute("UPDATE agent_tasks SET state='accepted',lease_owner=NULL,lease_until=0 WHERE id=%s AND state IN ('running','interrupted')",(job['task_id'],))
        await self.store.call(release)

    async def _retry(self,job):
        await self.store.call(lambda c:c.execute("UPDATE agent_jobs SET state='queued',owner=NULL,lease_until=0 WHERE task_id=%s AND owner=%s",(job['task_id'],job['owner'])))

    async def complete_external(self,scope,task_id):
        """A completed legacy resume must release its original durable session head."""
        def complete(c):
            job=c.execute('SELECT credential_ref FROM agent_jobs WHERE task_id=%s AND scope=%s FOR UPDATE',(task_id,scope)).fetchone()
            if not job:return None
            task=c.execute('SELECT state FROM agent_tasks WHERE id=%s AND scope=%s FOR SHARE',(task_id,scope)).fetchone()
            if not task or task['state'] in ('accepted','running','waiting_auth'):return None
            c.execute("UPDATE agent_jobs SET state='done',owner=NULL,lease_until=0 WHERE task_id=%s",(task_id,))
            return job['credential_ref']
        reference=await self.store.call(complete)
        if reference:await self.vault.delete(reference)

    async def _worker(self):
        while not self.stopping:
            try:
                self.hub.queue.clear()
                job = await self.claim_next()
                if job is None:
                    try:
                        await asyncio.wait_for(self.hub.queue.wait(), timeout=1)
                    except TimeoutError:
                        pass
                    continue
                identifier = job['task_id']
                self.tasks[identifier] = asyncio.current_task()
                completed=False
                try:
                    previous=await self.store.get(job['scope'],identifier)
                    if previous['state'] not in ('accepted','running') and not previous.get('recovery_required'):
                        if await self._finish(job,'done'):
                            await self.vault.delete(job['credential_ref'])
                        continue
                    authorization = await self.vault.get(job['scope'], job['credential_ref'])
                    await self.execute(job, authorization)
                except AgentError as exc:
                    if exc.code in ('SESSION_BUSY','TASK_BUSY'):
                        await self._retry(job)
                        await asyncio.sleep(.1)
                        continue
                    if exc.code == 'REAUTH_REQUIRED':
                        if await self._reauth(job,str(exc)):
                            await self.vault.delete(job['credential_ref'])
                        continue
                    completed=bool(await self._finish(job, 'failed'))
                except asyncio.CancelledError:
                    await asyncio.shield(self._release_cancelled(job))
                    raise
                except Exception:
                    completed=bool(await self._finish(job, 'failed'))
                else:
                    completed=bool(await self._finish(job, 'done'))
                finally:
                    self.tasks.pop(identifier, None)
                if completed and (await self.store.get(job['scope'], identifier))['state'] not in ('accepted','running'):
                    await self.vault.delete(job['credential_ref'])
            except asyncio.CancelledError:
                raise
            except Exception:
                # Storage outages do not spin or log credentials/provider response bodies.
                await asyncio.sleep(1)

    async def close(self):
        self.stopping = True
        for task in self.runners:
            task.cancel()
        await asyncio.gather(*self.runners, return_exceptions=True)
