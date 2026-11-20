"""P6 HTTP stage profiling on the existing isolated JMeter harness."""
import argparse
import asyncio
import hashlib
import json
import shutil
import threading
import time
import secrets
import httpx
from pathlib import Path

from run_perf import PerfRun, patch_source, windows_probe, database_probe, redis_stats, verify_timeout_outbox
from run_scale_jmeter import ROOT, HERE, wave, seed_users, write
from redis.asyncio import Redis
from p6_plan import make_warm_plan, seed_warmup


async def pending_probe(run, stop):
    async with Redis.from_url(run.redis_url, decode_responses=True) as redis, httpx.AsyncClient(
            base_url=run.java_url, timeout=4, trust_env=False) as client:
        last_mq = 0
        last_outbox = 0
        index = 0
        while not stop.is_set():
            row = {'at': int(time.time() * 1000), 'reserved_pending': await redis.zcard('seckill:admission:pending'),
                   'unconfirmed': await redis.zcard('seckill:admission:unconfirmed')}
            with (run.path / 'admission-queue.jsonl').open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(row) + '\n')
            if time.monotonic() - last_outbox >= 5:
                counts = await asyncio.to_thread(run.sql, """SELECT topic,COUNT(*),
                    SUM(status=0),SUM(status=1),COALESCE(MAX(IF(status=0,TIMESTAMPDIFF(SECOND,create_time,NOW()),0)),0)
                    FROM tb_mq_message GROUP BY topic;""")
                with (run.path / 'outbox-queue.jsonl').open('a', encoding='utf-8') as stream:
                    stream.write(json.dumps({'at': int(time.time() * 1000), 'topics': counts}) + '\n')
                last_outbox = time.monotonic()
            if time.monotonic() - last_mq > 7:
                index += 1
                stamp = int(time.time() * 1000)
                try:
                    response = await client.get('/internal/ops/mq',
                        headers={'X-Ops-Token': run.env['HMDP_OPS_ADMIN_TOKEN']},
                        params={'start': stamp - 60000, 'end': stamp, 'voucherId': 98201, 'ip': '127.0.0.1'})
                    value = {'at': stamp, 'http': response.status_code, 'observation': response.json()}
                except Exception as error:
                    value = {'at': stamp, 'error': type(error).__name__}
                with (run.path / 'load-mq.jsonl').open('a', encoding='utf-8') as stream:
                    stream.write(json.dumps(value, ensure_ascii=False) + '\n')
                last_mq = time.monotonic()
            try:
                await asyncio.wait_for(stop.wait(), timeout=.5)
            except TimeoutError:
                pass


class HttpPerfRun(PerfRun):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.env['HMDP_OPS_ADMIN_TOKEN'] = secrets.token_urlsafe(32)
        # Enable read-only observations only in the isolated performance process.
        self.env['HMDP_OPS_ENABLED'] = 'true'
        write(self.path / 'api_usage.json', {'scope': 'P6 Java performance only', 'actual_requests': 0,
                                           'model_calls_permitted': False})

    def create(self, role, image, options, command, config=None):
        if getattr(self, 'storage_profile', 'tmpfs') == 'disk' and role in ('mysql', 'redis', 'broker'):
            destination = {'mysql': '/var/lib/mysql', 'redis': '/data', 'broker': '/home/rocketmq/store'}[role]
            volume = self.project + '-' + role + '-data'
            if self.command(['docker', 'volume', 'inspect', volume], check=False).returncode == 0:
                raise RuntimeError('Refuse to reuse an existing data volume')
            self.command(['docker', 'volume', 'create', '--label', 'localperks.validation.run=' + self.id,
                          '--label', 'com.docker.compose.project=' + self.project, volume])
            self.register('volume', volume, 'Fresh disk-backed data volume, exclusively owned by this run')
            adjusted = []
            index = 0
            while index < len(options):
                if options[index] == '--tmpfs' and options[index + 1].split(':')[0] == destination:
                    index += 2
                else:
                    adjusted.append(options[index])
                    index += 1
            options = adjusted + ['--mount', 'type=volume,source=' + volume + ',target=' + destination]
            if role == 'mysql':
                command = [arg for arg in command if arg != '--skip-log-bin'] + [
                    '--log-bin=mysql-bin', '--server-id=1', '--sync-binlog=1', '--innodb-flush-log-at-trx-commit=1']
            elif role == 'redis':
                command = list(command)
                command[command.index('--appendonly') + 1] = 'yes'
                command += ['--appendfsync', 'everysec']
            elif config:
                config.write_text(config.read_text(encoding='utf-8').replace('ASYNC_FLUSH', 'SYNC_FLUSH'), encoding='utf-8')
                # A fresh named volume starts root-owned; the Broker image runs as rocketmq.
                # This helper mounts only the just-created, run-labelled volume.
                self.command(['docker', 'run', '--rm', '--network', 'none', '--user', '0:0',
                    '--label', 'localperks.validation.run=' + self.id,
                    '--mount', 'type=volume,source=' + volume + ',target=' + destination,
                    image, 'sh', '-c', 'chown -R rocketmq:rocketmq /home/rocketmq/store'])
        return super().create(role, image, options, command, config=config)

    def build(self):
        # Reuse only a byte-identical instrumented source/configuration, retaining the original build.
        sources = list((ROOT / 'src/main').rglob('*')) + [ROOT / 'pom.xml', HERE / 'PerfProbe.java',
            HERE / 'HttpRequestProbe.java', HERE / 'run_perf.py', HERE / 'run_http_perf.py']
        fingerprint = hashlib.sha256()
        for source in sorted(path for path in sources if path.is_file()):
            fingerprint.update(str(source.relative_to(ROOT)).encode())
            fingerprint.update(source.read_bytes())
        if getattr(self, 'baseline_source', None):
            fingerprint.update(b'controlled-original-http-path')
            for source in sorted(self.baseline_source.rglob('*.java')):
                fingerprint.update(source.read_bytes())
        fingerprint.update(getattr(self, 'script_mode', 'immutable').encode())
        if getattr(self, 'precise', False):
            for source in sorted([*(HERE / 'precise').glob('*.java'), HERE / 'precise_source.py']):
                fingerprint.update(source.read_bytes())
        signature = fingerprint.hexdigest()
        cache = ROOT / 'artifacts/test-runs/.tools/p6-build'
        record = cache / (signature + '.json')
        if self.__class__ is HttpPerfRun and record.is_file():
            info = json.loads(record.read_text(encoding='utf-8'))
            jar = cache / (signature + '.jar')
            if info['signature'] == signature and jar.is_file() and hashlib.sha256(jar.read_bytes()).hexdigest() == info['jar_sha256']:
                self.jar = jar
                self.measure_timeout_outbox = True
                write(self.path / 'versions.json', {**info, 'reused_build': True,
                    'normal_volumes_before': self.command(['docker', 'volume', 'ls', '--format', '{{.Name}}']).stdout.splitlines(),
                    'jmx': self.plan.name, 'jmx_sha256': hashlib.sha256(self.plan.read_bytes()).hexdigest()})
                return
        super().build()
        limits_path = self.path / 'isolated-limits.json'
        limits = json.loads(limits_path.read_text(encoding='utf-8'))
        limits['scale_global_ip_user'] = [50000, 50000, 5]
        actual_aspect = self.path / 'temp/isolated-build/src/main/java/com/hmdp/ratelimit/RateLimitAspect.java'
        limits['aspect_after_sha256'] = hashlib.sha256(actual_aspect.read_bytes()).hexdigest()
        write(limits_path, limits)
        if self.__class__ is HttpPerfRun:
            cache.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.jar, cache / (signature + '.jar'))
            source_zip = Path(shutil.make_archive(str(cache / (signature + '.sources')), 'zip',
                self.path / 'temp/isolated-build/src/main'))
            write(record, {'signature': signature, 'jar_sha256': hashlib.sha256(self.jar.read_bytes()).hexdigest(),
                           'original_build': self.id, 'compiled_sources': str(source_zip.relative_to(ROOT)),
                           'compiled_sources_sha256': hashlib.sha256(source_zip.read_bytes()).hexdigest()})
            versions = json.loads((self.path / 'versions.json').read_text(encoding='utf-8'))
            versions.update(compiled_sources=str(source_zip.relative_to(ROOT)),
                            compiled_sources_sha256=hashlib.sha256(source_zip.read_bytes()).hexdigest())
            write(self.path / 'versions.json', versions)

    def prepare_source(self, work):
        if getattr(self, 'baseline_source', None):
            java = work / 'src/main/java/com/hmdp'
            for relative in ('service/impl/VoucherOrderServiceImpl.java', 'mq/SeckillOrderMessageService.java'):
                shutil.copy2(self.baseline_source / relative, java / relative)
            rate = self.baseline_source / 'ratelimit/SlidingWindowRateLimiter.java'
            if rate.exists():
                shutil.copy2(rate, java / 'ratelimit/SlidingWindowRateLimiter.java')
            for relative in ('config/SeckillDataSourceConfig.java', 'config/SeckillScriptInitializer.java',
                             'mq/SeckillAdmissionRepository.java', 'mq/SeckillAdmissionState.java',
                             'mq/SeckillOutboxWriter.java', 'mq/SeckillOutboxBatcher.java'):
                target = (java / relative).resolve()
                if not target.is_relative_to((self.path / 'temp').resolve()):
                    raise ValueError('Baseline reconstruction must stay inside the isolated build')
                target.unlink(missing_ok=True)
            configuration = work / 'src/main/resources/application.yaml'
            text = configuration.read_text(encoding='utf-8').replace('retry-fixed-delay: 100', 'retry-fixed-delay: 5000')
            configuration.write_text(text, encoding='utf-8')
        if getattr(self, 'script_mode', 'immutable') == 'resource':
            patch_source(work / 'src/main/java/com/hmdp/utils/RedisScripts.java', [
                ('            DefaultRedisScript<T> script = new DefaultRedisScript<>(\n                    StreamUtils.copyToString(stream, StandardCharsets.UTF_8), resultType);',
                 '            DefaultRedisScript<T> script = new DefaultRedisScript<>();\n            script.setLocation(new ClassPathResource(name));\n            script.setResultType(resultType);'),
            ])
        super().prepare_source(work)
        java = work / 'src/main/java/com/hmdp'
        service = java / 'service/impl/VoucherOrderServiceImpl.java'
        modern = 'SeckillAdmissionRepository admission' in service.read_text(encoding='utf-8')
        prechecks = [
            ('        return admission.findOrder(userId, voucherId);' if modern else '        return query().eq("user_id", userId).eq("voucher_id", voucherId).one();',
             '        long started = System.nanoTime();\n        try {\n        ' + ('return admission.findOrder(userId, voucherId);' if modern else 'return query().eq("user_id", userId).eq("voucher_id", voucherId).one();') + '\n        } finally { com.hmdp.perf.PerfProbe.duration("http_existing_order_sql", started); }'),
            ('        SeckillVoucher activity = admission.activity(voucherId);' if modern else '            SeckillVoucher activity = seckillVoucherService.getById(voucherId);',
             '        long activityStarted = System.nanoTime();\n' + ('        SeckillVoucher activity = admission.activity(voucherId);' if modern else '            SeckillVoucher activity = seckillVoucherService.getById(voucherId);') + '\n        com.hmdp.perf.PerfProbe.duration("http_activity_sql", activityStarted);'),
        ]
        if 'SeckillAdmissionRepository.Eligibility eligibility' in service.read_text(encoding='utf-8'):
            prechecks = [
                ('        SeckillAdmissionRepository.Eligibility eligibility = admission.inspect(userId, voucherId);',
                 '        long precheckStarted = System.nanoTime();\n        SeckillAdmissionRepository.Eligibility eligibility = admission.inspect(userId, voucherId);\n        com.hmdp.perf.PerfProbe.duration("http_precheck_sql", precheckStarted);')
            ]
        elif 'admissionState.purchased(voucherId, userId)' in service.read_text(encoding='utf-8'):
            # Redis-only admission has no foreground SQL stage to instrument.
            prechecks = []
        patch_source(service, prechecks + [
            ('            seckillOrderMessageService.createAndSend(new VoucherOrderMessage(orderId, userId, voucherId));',
             '            long submitStarted = System.nanoTime();\n            seckillOrderMessageService.createAndSend(new VoucherOrderMessage(orderId, userId, voucherId));\n            com.hmdp.perf.PerfProbe.duration("http_durable_submit", submitStarted);'),
        ])
        direct = 'private SendResult sendConfirmed' in (java / 'mq/SeckillOrderMessageService.java').read_text(encoding='utf-8')
        if direct:
            self.admission_mode = 'broker-confirmed'
        elif modern:
            patch_source(java / 'mq/SeckillOrderMessageService.java', [
                ('        admission.persist(row);', '        long persistStarted = System.nanoTime();\n        admission.persist(row);\n        com.hmdp.perf.PerfProbe.duration("http_outbox_insert", persistStarted);'),
                ('        offer(() -> publish(admission.findMessage(message.getOrderId(), message.getUserId())));',
                 '        long dispatchStarted = System.nanoTime();\n        offer(() -> publish(admission.findMessage(message.getOrderId(), message.getUserId())));\n        com.hmdp.perf.PerfProbe.duration("http_dispatch_call", dispatchStarted);'),
            ])
        else:
            patch_source(java / 'mq/SeckillOrderMessageService.java', [
            ('        mqMessageMapper.insert(mqMessage);',
             '        long persistStarted = System.nanoTime();\n        mqMessageMapper.insert(mqMessage);\n        com.hmdp.perf.PerfProbe.duration("http_outbox_insert", persistStarted);'),
            ('        sendAsync(mqMessage, message);\n    }\n\n    public void retryPendingMessages()',
             '        long dispatchStarted = System.nanoTime();\n        sendAsync(mqMessage, message);\n        com.hmdp.perf.PerfProbe.duration("http_dispatch_call", dispatchStarted);\n    }\n\n    public void retryPendingMessages()'),
            ])
        patch_source(java / 'utils/RefreshTokenInterceptor.java', [
            ('        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);',
             '        long identityStarted = System.nanoTime();\n        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);\n        com.hmdp.perf.PerfProbe.duration("identity_redis_read", identityStarted);'),
        ])
        shutil.copy2(HERE / 'HttpRequestProbe.java', java / 'perf/HttpRequestProbe.java')
        aspect = java / 'ratelimit/RateLimitAspect.java'
        source = aspect.read_text(encoding='utf-8')
        aspect.write_text(source.replace('? 5000 : rule.limit()', '? 50000 : rule.limit()'), encoding='utf-8')
        if getattr(self, 'precise', False):
            from precise_source import instrument
            instrument(java)


async def run_tier(args):
    if args.disable_eligibility_batch and not (ROOT / 'src/main/java/com/hmdp/mq/SeckillEligibilityReader.java').exists():
        raise ValueError('Foreground SQL eligibility has been removed; no eligibility batching switch remains')
    arrival = args.rps is not None
    oversell = args.oversell
    users = max(100, int(args.rps * args.seconds * 1.4)) if arrival else 500 if oversell else args.threads
    run = HttpPerfRun(users, args.label, arrivals=arrival)
    run.storage_profile = args.storage
    run.jmeter_force_exit = True
    run.load_timeout_seconds = args.seconds + 95 if arrival else 155
    run.env['HMDP_ELIGIBILITY_BATCH_ENABLED'] = 'false' if args.disable_eligibility_batch else 'true'
    run.env['HMDP_BROKER_BATCH_ENABLED'] = 'false' if args.disable_broker_batch else 'true'
    run.direct_admission = 'private SendResult sendConfirmed' in (ROOT / 'src/main/java/com/hmdp/mq/SeckillOrderMessageService.java').read_text(encoding='utf-8') and not args.baseline_source
    if args.disable_outbox_batch and run.direct_admission:
        raise ValueError('Admission Outbox has been removed; --disable-outbox-batch is only for archived source')
    run.env['HMDP_OUTBOX_BATCH_ENABLED'] = 'false' if args.disable_outbox_batch else 'true'
    run.precise = args.precise
    run.script_mode = args.script_mode
    run.baseline_source = args.baseline_source.resolve(strict=True) if args.baseline_source else None
    if arrival:
        run.allow_backpressure = True
        run.plan = run.path / 'p6-arrivals.jmx'
        source = (HERE / 'perf_seckill_arrivals.jmx').read_text(encoding='utf-8')
        source = source.replace('rate(20/sec) random_arrivals(60 sec) rate(20/sec) pause(25 sec)',
                                f'rate({args.rps}/sec) random_arrivals({args.seconds} sec) rate({args.rps}/sec) pause(25 sec)')
        # An independently offered arrival schedule needs a load-generator safety ceiling.
        marker = '          <JSR223PostProcessor '
        guard = '''          <JSR223PreProcessor guiclass="TestBeanGUI" testclass="JSR223PreProcessor" testname="Stop if injector exceeds 512 active threads" enabled="true">
            <stringProp name="scriptLanguage">groovy</stringProp><stringProp name="cacheKey">true</stringProp>
            <stringProp name="script">if (ctx.getThreadGroup().numberOfActiveThreads() > 512) { log.error('P6_INJECTOR_THREAD_LIMIT'); ctx.getEngine().stopTest(true); }</stringProp>
          </JSR223PreProcessor><hashTree/>
'''
        guard = guard.replace('512', str(args.max_injector_threads))
        source = source.replace(marker, guard + marker)
        run.plan.write_text(source, encoding='utf-8')
    if args.warmup:
        make_warm_plan(run)
    write(run.path / 'p6-gate.json', {'label': args.label, 'mode': 'arrival' if arrival else 'oversell' if oversell else 'burst', 'rps': args.rps,
        'seconds': args.seconds if arrival else None, 'threads': None if arrival else args.threads,
        'p95_ms': 500, 'p99_ms': 1000, 'accepted_http_rps': 500, 'max_injector_threads': args.max_injector_threads,
        'backpressure_429_must_be_reported_separately': True, 'final_consistency_required': True})
    write(run.path / 'benchmark-context.json', {'baseline_source': str(run.baseline_source) if run.baseline_source else None,
        'storage_profile': args.storage, 'admission_mode': 'broker-confirmed' if run.direct_admission else 'sql-outbox',
        'broker_batch_enabled': not args.disable_broker_batch,
        'eligibility_batch_enabled': not args.disable_eligibility_batch,
        'outbox_batch_enabled': None if run.direct_admission else not args.disable_outbox_batch,
        'jmeter_exit_after_report': True,
        'declared_warmup_orders': 32 if args.warmup else 0, 'warmup_voucher': 98200 if args.warmup else None,
        'precise': args.precise, 'script_mode': args.script_mode,
        'warmup_gate': 'all 32 distinct orders visible before measured thread group starts' if args.warmup else None,
        'post_warmup_all_measured_requests_retained': True})
    shutil.copy2(run.path / 'p6-gate.json', run.path / 'perf-gate.json')
    frozen = run.path / 'source-before'
    for relative in ('service/impl/VoucherOrderServiceImpl.java', 'mq/SeckillOrderMessageService.java'):
        source = ROOT / 'src/main/java/com/hmdp' / relative
        target = frozen / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
    print('HTTP_PERF ' + str(run.path), flush=True)
    try:
        run.sample('before', guard=True)
        run.build()
        await run.infrastructure()
        write(run.path / 'storage-settings.json', {'profile': args.storage,
            'mysql_log_bin_sync_binlog_flush_log_at_commit': run.sql('SELECT @@log_bin,@@sync_binlog,@@innodb_flush_log_at_trx_commit;')[0],
            'broker_flush_disk_type': 'SYNC_FLUSH' if args.storage == 'disk' else 'ASYNC_FLUSH',
            'redis_appendonly': args.storage == 'disk', 'note': 'Disk profile uses newly labelled test volumes; no existing volume reused'})
        run.monitor_thread = threading.Thread(target=run.monitor, daemon=True)
        run.monitor_thread.start()
        await seed_users(run, users * 2)
        if args.precise:
            async with Redis.from_url(run.redis_url, decode_responses=True) as redis:
                await redis.config_set('latency-tracking-info-percentiles', '50 95 99')
        if args.warmup:
            await seed_warmup(run)
        await redis_stats(run, 'before')
        if args.precise:
            async with Redis.from_url(run.redis_url, decode_responses=True) as redis:
                write(run.path / 'redis-latency-before.json', {
                    'latencystats': await redis.info('latencystats'),
                    'lua_histogram': await redis.execute_command('LATENCY', 'HISTOGRAM', 'EVALSHA', 'EVAL')})
        stop = asyncio.Event()
        probes = [asyncio.create_task(probe(run, stop)) for probe in (windows_probe, database_probe, pending_probe)]
        try:
            profile = ('arrival' if arrival else 'throughput', users, 2, users * 2 + 20, 98201, users * 2)
            if oversell:
                run.drain_target = 30
                profile = ('oversell', 500, 1, 30, 98101, 30)
            result, _ = await wave(run, args.jmeter.resolve(strict=True), profile, 32 if args.warmup else 0)
            result['timeout_outbox'] = await verify_timeout_outbox(run, profile[4], result['mysql_orders'])
            result['pass'] &= (result['timeout_outbox']['persisted'] == result['mysql_orders'] ==
                               result['timeout_outbox']['acknowledged'] + result['timeout_outbox']['local'])
            result['challenge'] = result['pass'] and result['p95_ms'] <= 500 and result['p99_ms'] <= 1000 and result['http_response_rps'] >= 500
            if arrival:
                result.update(concurrency=None, fixture_user_capacity=users * 2,
                    target_rps=args.rps, scheduled_seconds=args.seconds,
                    accepted_response_rps=result['http_response_rps'] * result['http_accepted'] / max(1, result['attempted']),
                    offered_rate_met=abs(result['attempted'] - args.rps * args.seconds) <= args.rps * args.seconds * .1)
                result['challenge'] &= result['http_429'] == 0 and result['offered_rate_met']
            write(run.path / 'perf-result.json', result)
        finally:
            stop.set()
            await asyncio.gather(*probes)
            await redis_stats(run, 'after')
            if args.precise:
                async with Redis.from_url(run.redis_url, decode_responses=True) as redis:
                    write(run.path / 'redis-latency-after.json', {
                        'latencystats': await redis.info('latencystats'),
                        'lua_histogram': await redis.execute_command('LATENCY', 'HISTOGRAM', 'EVALSHA', 'EVAL')})
            dump = run.command(['jcmd', run.processes[0].pid, 'JFR.dump', 'name=load',
                                'filename=' + (run.path / 'java.jfr').as_posix()], check=False)
            (run.path / 'jfr-dump.log').write_text(dump.stdout + dump.stderr, encoding='utf-8')
            run.sample('end')
    except BaseException as error:
        write(run.path / 'perf-harness-failure.json', {'error': type(error).__name__, 'detail': str(error)})
        raise
    finally:
        states = {}
        for role, identifier in run.containers.items():
            raw = run.command(['docker', 'inspect', '--format', '{{json .State}}', identifier], check=False)
            if raw.returncode == 0:
                states[role] = json.loads(raw.stdout)
        write(run.path / 'container-final-state.json', states)
        run.cleanup()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jmeter', type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--threads', type=int, choices=(100, 200, 300, 500))
    mode.add_argument('--rps', type=int, choices=(20, 50, 100, 200, 500))
    mode.add_argument('--oversell', action='store_true')
    parser.add_argument('--seconds', type=int, default=30)
    parser.add_argument('--storage', choices=('tmpfs', 'disk'), default='tmpfs')
    parser.add_argument('--disable-outbox-batch', action='store_true')
    parser.add_argument('--disable-broker-batch', action='store_true', help='Controlled original per-message Broker confirmation')
    parser.add_argument('--disable-eligibility-batch', action='store_true', help='Controlled original per-request eligibility query')
    parser.add_argument('--max-injector-threads', type=int, choices=(512, 1024), default=512)
    parser.add_argument('--warmup', action='store_true', help='Declare and drain 32 setup orders before measurement')
    parser.add_argument('--baseline-source', type=Path, help='Original handler/publisher snapshots, isolated build only')
    parser.add_argument('--precise', action='store_true', help='Capture executor queues and per-request connection/command phases')
    parser.add_argument('--script-mode', choices=('immutable', 'resource'), default='immutable')
    parser.add_argument('--label', required=True)
    asyncio.run(run_tier(parser.parse_args()))
