"""Run one preregistered performance tier in a new manifest-owned stack."""
import argparse
import asyncio
import json
import re
import shutil
import time
from pathlib import Path

from redis.asyncio import Redis

from run_scale_jmeter import HERE, ROOT, ScaleRun, seed_users, wave, write

GATE = {'unexpected': 0, 'http_5xx': 0, 'http_429': 0, 'http_p95_max_ms': 5000,
        'all_distinct_orders_visible': True, 'mq_lag_and_inflight': 0,
        'drain_observation_seconds': 60, 'loops_per_thread': 2}


class PerfRun(ScaleRun):
    def __init__(self, users, label, arrivals=False, workers=None):
        super().__init__()
        self.users, self.label = users, label
        self.open_arrivals = arrivals
        self.workers = workers
        self.plan = HERE / ('perf_seckill_arrivals.jmx' if arrivals else 'scale_seckill_warm.jmx')
        self.drain_probes = GATE['drain_observation_seconds'] * 2
        self.drain_target = users * GATE['loops_per_thread']
        rel = self.path.relative_to(ROOT).as_posix()
        self.java_vm_options = ['-Xlog:gc*:stdout:time,uptime,level,tags',
                                f'-XX:StartFlightRecording=name=load,settings=profile,maxsize=64m,filename={rel}/java.jfr']
        self.java_app_options = ['--server.tomcat.mbeanregistry.enabled=true']
        self.jmeter_vm_options = ('-Xlog:gc*:stdout:time,uptime,level,tags '
                                  f'-XX:StartFlightRecording=settings=profile,maxsize=48m,filename={self.path.as_posix()}/jmeter.jfr')
        self.env.update(SHOPLINK_PROBE_METRICS=str(self.path / 'jvm-metrics.jsonl'),
                        SHOPLINK_PROBE_TIMINGS=str(self.path / 'stages-timing.csv'))
        write(self.path / 'perf-gate.json', {'label': label, 'threads': users, 'gate': GATE, 'isolated_consumer_workers': workers,
              'arrivals': {'target_rps': 20, 'seconds': 60, 'acceptable_count': [1080, 1320]} if arrivals else None,
              'topology': 'single Windows host, separate native JVMs; new Docker stack per tier'})

    def prepare_source(self, work):
        java = work / 'src/main/java/com/hmdp'
        if self.workers is not None:
            listener = java / 'mq/SeckillOrderConsumer.java'
            source = listener.read_text(encoding='utf-8')
            source = re.sub(r'(?m)^        consumeThread(?:Number|Max) = \d+,\n', '', source)
            listener.write_text(source, encoding='utf-8')
            anchor = '        maxReconsumeTimes = CONSUMER_MAX_RECONSUME_TIMES'
            patch_source(listener, [(anchor,
                f'        consumeThreadNumber = {self.workers},\n        consumeThreadMax = {self.workers},\n' + anchor)])
        target = java / 'perf' / 'PerfProbe.java'
        target.parent.mkdir()
        shutil.copy2(HERE / 'PerfProbe.java', target)
        patch_source(java / 'service/impl/VoucherOrderServiceImpl.java', [
            ('        Long result = stringRedisTemplate.execute(\n                SECKILL_SCRIPT,',
             '        long luaStarted = System.nanoTime();\n        Long result = stringRedisTemplate.execute(\n                SECKILL_SCRIPT,'),
            ('        int code = result == null ? 1 : result.intValue();',
             '        com.hmdp.perf.PerfProbe.duration("reserve_lua_rtt", luaStarted);\n        int code = result == null ? 1 : result.intValue();'),
            ('        boolean locked = redisLock.tryLock();',
             '        long lockStarted = System.nanoTime();\n        boolean locked = redisLock.tryLock();\n        com.hmdp.perf.PerfProbe.duration("redisson_lock", lockStarted);'),
            ('            boolean stockDeducted = seckillVoucherService.update()',
             '            long stockStarted = System.nanoTime();\n            boolean stockDeducted = seckillVoucherService.update()'),
            ('            if (!stockDeducted) {',
             '            com.hmdp.perf.PerfProbe.duration("sql_hot_stock_update", stockStarted);\n            if (!stockDeducted) {'),
        ])
        path = java / 'service/impl/VoucherOrderServiceImpl.java'
        source = path.read_text(encoding='utf-8')
        if '    private void sendOrderTimeoutMessage(' in source:
            patch_source(path, [
                ('        rocketMQTemplate.syncSend(\n                ORDER_TIMEOUT_TOPIC,',
                 '        long closeStarted = System.nanoTime();\n        try {\n        rocketMQTemplate.syncSend(\n                ORDER_TIMEOUT_TOPIC,'),
                ('                ORDER_TIMEOUT_DELAY_LEVEL\n        );\n    }',
                 '                ORDER_TIMEOUT_DELAY_LEVEL\n        );\n        } finally {\n            com.hmdp.perf.PerfProbe.duration("close_mq_sync", closeStarted);\n        }\n    }'),
            ])
        patch_source(java / 'ratelimit/SlidingWindowRateLimiter.java', [
            ('        List result = stringRedisTemplate.execute(SLIDING_WINDOW_SCRIPT, keys, args.toArray());',
             '        long started = System.nanoTime();\n        List result = stringRedisTemplate.execute(SLIDING_WINDOW_SCRIPT, keys, args.toArray());\n        com.hmdp.perf.PerfProbe.duration("rate_lua_rtt", started);'),
        ])
        publisher = java / 'mq/SeckillOrderMessageService.java'
        if 'private SendResult sendConfirmed' in publisher.read_text(encoding='utf-8'):
            patch_source(publisher, [
                ('    private SendResult sendConfirmed(VoucherOrderMessage message) {',
                 '    private SendResult sendConfirmed(VoucherOrderMessage message) {\n        long publishStarted = System.nanoTime();'),
                ('        return result;',
                 '        com.hmdp.perf.PerfProbe.duration("seckill_producer_ack", publishStarted);\n        return result;'),
            ])
        elif 'SeckillAdmissionRepository admission' in publisher.read_text(encoding='utf-8'):
            patch_source(publisher, [
                ('    private void sendAsync(MqMessage row, VoucherOrderMessage message) {',
                 '    private void sendAsync(MqMessage row, VoucherOrderMessage message) {\n        long publishStarted = System.nanoTime();'),
                ('                        admission.sent(row.getId(), result.getMsgId());',
                 '                        com.hmdp.perf.PerfProbe.duration("seckill_producer_ack", publishStarted);\n                        long callbackStarted = System.nanoTime();\n                        admission.sent(row.getId(), result.getMsgId());\n                        com.hmdp.perf.PerfProbe.duration("seckill_ack_sql", callbackStarted);'),
                ('                    admission.uncertain(row, error == null ? "unknown send outcome" : error.toString());',
                 '                    com.hmdp.perf.PerfProbe.duration("seckill_producer_failed", publishStarted);\n                    admission.uncertain(row, error == null ? "unknown send outcome" : error.toString());'),
            ])
        else:
            patch_source(publisher, [
            ('    private void sendAsync(MqMessage mqMessage, VoucherOrderMessage message) {',
             '    private void sendAsync(MqMessage mqMessage, VoucherOrderMessage message) {\n        long publishStarted = System.nanoTime();'),
            ('                            markSent(mqMessage.getId(), sendResult.getMsgId());',
             '                            com.hmdp.perf.PerfProbe.duration("seckill_producer_ack", publishStarted);\n                            long callbackStarted = System.nanoTime();\n                            markSent(mqMessage.getId(), sendResult.getMsgId());\n                            com.hmdp.perf.PerfProbe.duration("seckill_ack_sql", callbackStarted);'),
            ('                            markSendFailed(mqMessage, throwable);',
             '                            com.hmdp.perf.PerfProbe.duration("seckill_producer_failed", publishStarted);\n                            markSendFailed(mqMessage, throwable);'),
            ])
        timeouts = java / 'mq/OrderTimeoutMessageService.java'
        eligibility = java / 'mq/SeckillEligibilityReader.java'
        if eligibility.exists():
            patch_source(eligibility, [
                ('        SeckillAdmissionRepository.Eligibility[] result = jdbc.query(sql.toString(), arguments.toArray(), rows -> {',
                 '        long readStarted = System.nanoTime();\n        SeckillAdmissionRepository.Eligibility[] result = jdbc.query(sql.toString(), arguments.toArray(), rows -> {'),
                ('        return result;',
                 '        com.hmdp.perf.PerfProbe.duration("eligibility_batch_query", readStarted);\n        com.hmdp.perf.PerfProbe.duration("eligibility_batch_members_" + batch.size(), readStarted);\n        return result;'),
            ])
        batch_sender = java / 'mq/SeckillBrokerBatchSender.java'
        if batch_sender.exists():
            patch_source(batch_sender, [
                ('                SendResult result = rocket.syncSend(SECKILL_ORDER_TOPIC, messages, SEND_TIMEOUT_MILLIS);',
                 '                long batchStarted = System.nanoTime();\n                SendResult result = rocket.syncSend(SECKILL_ORDER_TOPIC, messages, SEND_TIMEOUT_MILLIS);\n                com.hmdp.perf.PerfProbe.duration("broker_batch_rpc", batchStarted);\n                com.hmdp.perf.PerfProbe.duration("broker_batch_members_" + batch.size(), batchStarted);'),
            ])
        self.measure_timeout_outbox = timeouts.exists()
        if self.measure_timeout_outbox:
            patch_source(timeouts, [
                ('        SendCallback callback = new SendCallback() {',
                 '        long closeStarted = System.nanoTime();\n        SendCallback callback = new SendCallback() {'),
                ('                    if (result.getSendStatus() == SendStatus.SEND_OK) {',
                 '                    com.hmdp.perf.PerfProbe.duration("close_mq_async_ack", closeStarted);\n                    if (result.getSendStatus() == SendStatus.SEND_OK) {'),
                ('                    retry(row, deadline, error.getClass().getSimpleName() + ": " + error.getMessage());',
                 '                    com.hmdp.perf.PerfProbe.duration("close_mq_async_failed", closeStarted);\n                    retry(row, deadline, error.getClass().getSimpleName() + ": " + error.getMessage());'),
            ])


def patch_source(path, replacements):
    source = path.read_text(encoding='utf-8')
    for before, after in replacements:
        if source.count(before) != 1:
            raise ValueError(f'Isolated instrumentation anchor changed: {path.name}: {before[:40]}')
        source = source.replace(before, after)
    path.write_text(source, encoding='utf-8')


def append(path, text):
    with path.open('a', encoding='utf-8') as stream:
        stream.write(text + '\n')


async def windows_probe(run, stop):
    app_pid = run.processes[0].pid
    first_load = None
    dumps = set()
    while not stop.is_set():
        try:
            result = await asyncio.to_thread(run.command, ['pwsh', '-NoProfile', '-File', HERE / 'sample_perf.ps1',
                                                         '-AppPid', app_pid, '-Port', run.java_port], timeout=8)
            row = json.loads(result.stdout)
            append(run.path / 'process-network.jsonl', json.dumps(row))
            if any(item['role'] == 'jmeter' for item in row['processes']) and first_load is None:
                first_load = time.monotonic()
            if first_load is not None:
                for offset in (5, 7, 10, 12, 16, 32):
                    if time.monotonic() - first_load >= offset and offset not in dumps:
                        dumps.add(offset)
                        for process in row['processes']:
                            result = await asyncio.to_thread(run.command, ['jcmd', process['pid'], 'Thread.print', '-l'],
                                                             timeout=12, check=False)
                            (run.path / f"{process['role']}-threads-{offset}.txt").write_text(
                                f"sample_epoch_ms={row['at']}\n" + result.stdout + result.stderr, encoding='utf-8')
        except Exception as error:
            append(run.path / 'probe-errors.log', 'windows:' + type(error).__name__)
        await asyncio.sleep(1)


async def database_probe(run, stop):
    while not stop.is_set():
        try:
            rows = await asyncio.to_thread(run.sql, """
              SELECT 'trx', trx_id, trx_state, TIMESTAMPDIFF(SECOND,trx_started,NOW()), trx_rows_locked,
                     trx_rows_modified, COALESCE(LEFT(trx_query,160),'idle')
              FROM information_schema.innodb_trx
              ORDER BY trx_started LIMIT 65;
              SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_running','Threads_connected',
               'Innodb_row_lock_current_waits','Innodb_row_lock_time','Innodb_row_lock_waits','Slow_queries');
            """)
            stamp = int(time.time() * 1000)
            append(run.path / 'mysql-live.jsonl', json.dumps({'at': stamp, 'rows': rows}))
            if any(len(row) > 2 and row[2] == 'LOCK WAIT' for row in rows):
                target = run.path / 'innodb-locks.log'
                if not target.exists():
                    raw = await asyncio.to_thread(run.sql, 'SHOW ENGINE INNODB STATUS;')
                    target.write_text(f'sample_epoch_ms={stamp}\n' + '\n'.join('\t'.join(row) for row in raw), encoding='utf-8')
        except Exception as error:
            append(run.path / 'probe-errors.log', 'mysql:' + type(error).__name__)
        await asyncio.sleep(1)


async def redis_stats(run, label):
    async with Redis.from_url(run.redis_url, decode_responses=True) as redis:
        row = {key: await redis.info(key) for key in ('clients', 'commandstats', 'stats')}
        row['slowlog'] = [{'id': entry['id'], 'start_time': entry['start_time'], 'duration': entry['duration']}
                          for entry in await redis.slowlog_get(16)]
    write(run.path / f'redis-{label}.json', row)


async def verify_timeout_outbox(run, voucher, target):
    end = time.monotonic() + 45
    while True:
        rows = await asyncio.to_thread(run.sql, f"""SELECT COUNT(*),
          COALESCE(SUM(status=1 AND message_id<>'local-timeout'),0),COALESCE(SUM(message_id='local-timeout'),0)
          FROM tb_mq_message WHERE voucher_id={voucher} AND topic='voucher-order-timeout-topic'""")
        persisted, acknowledged, local = map(int, rows[0])
        if acknowledged + local >= target or time.monotonic() >= end:
            return {'persisted': persisted, 'acknowledged': acknowledged, 'local': local}
        await asyncio.sleep(.5)


async def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--jmeter', type=Path, required=True)
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument('--threads', type=int, choices=(100, 150, 200, 300, 500))
    mode.add_argument('--arrival', action='store_true')
    p.add_argument('--label', required=True)
    p.add_argument('--workers', type=int, choices=(4, 6))
    args = p.parse_args()
    users = 750 if args.arrival else args.threads
    run = PerfRun(users, args.label, args.arrival, args.workers)
    jmeter = args.jmeter.resolve(strict=True)
    print(f'PERF {args.label} {"open" if args.arrival else users} {run.path}', flush=True)
    try:
        run.sample('before', guard=True)
        run.build()
        await run.infrastructure()
        await seed_users(run, users * 2)
        await redis_stats(run, 'before')
        stop = asyncio.Event()
        probes = [asyncio.create_task(item(run, stop)) for item in (windows_probe, database_probe)]
        try:
            result, _ = await wave(run, jmeter, ('arrival' if args.arrival else 'throughput', users, 2,
                                                 users * 2 + 20, 98201, users * 2), 0)
            result['pass'] &= result['p95_ms'] <= GATE['http_p95_max_ms']
            if args.arrival:
                result['pass'] &= 1080 <= result['attempted'] <= 1320
                result['concurrency'] = None
                result['target_rps'] = 20
            if run.measure_timeout_outbox:
                outcome = await verify_timeout_outbox(run, 98201, result['mysql_orders'])
                result['timeout_outbox'] = outcome
                result['pass'] &= outcome['persisted'] == outcome['acknowledged'] == result['mysql_orders']
            write(run.path / 'perf-result.json', result)
            print(json.dumps(result), flush=True)
        finally:
            stop.set()
            await asyncio.gather(*probes)
            await redis_stats(run, 'after')
            raw = run.sql('SHOW ENGINE INNODB STATUS;')
            (run.path / 'innodb-final.log').write_text('\n'.join('\t'.join(row) for row in raw), encoding='utf-8')
            result = run.command(['jcmd', run.processes[0].pid, 'JFR.dump', 'name=load',
                                  'filename=' + (run.path / 'java.jfr').as_posix()], timeout=35, check=False)
            (run.path / 'jfr-dump.log').write_text(result.stdout + result.stderr, encoding='utf-8')
            run.sample('end')
    except Exception as error:
        run.failure = type(error).__name__
        write(run.path / 'perf-harness-failure.json', {'type': type(error).__name__, 'detail': str(error)})
        raise
    finally:
        run.cleanup()


if __name__ == '__main__':
    asyncio.run(main())
