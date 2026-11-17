"""JMeter scale tests in disposable Docker. Normal project artifacts and limits stay untouched."""
import argparse
import asyncio
import csv
import hashlib
import shutil
import threading
import time
from pathlib import Path

from run_k6 import Run, broker_progress, reset_window, seed_users, write
from run import pct
from run_jmeter import verify_states

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
PROFILES = [('warmup', 20, 1, 25, 91001, 20)]
PROFILES += [('oversell', 500, 1, 30, 98101, 30), ('limit', 500, 1, 520, 98301, 30)]
PROFILES += [('throughput', n, 2, n * 2 + 20, 98200 + i, n * 2) for i, n in enumerate((100, 200, 300, 500))]


class ScaleRun(Run):
    def build(self):
        work = self.path / 'temp' / 'isolated-build'
        shutil.copytree(ROOT / 'src/main', work / 'src/main')
        shutil.copy2(ROOT / 'pom.xml', work / 'pom.xml')
        aspect = work / 'src/main/java/com/hmdp/ratelimit/RateLimitAspect.java'
        before = aspect.read_text(encoding='utf-8')
        line = '            buckets.add(new RateLimitBucket(key, rule.limit(), rule.windowSeconds() * 1000L));'
        assert before.count(line) == 1
        after = before.replace(line, '''            // This source exists only inside the disposable scale-run build.
            boolean scaleFixture = rateLimit.key().equals("voucher-seckill")
                    && request.getLocalAddr().equals("127.0.0.1")
                    && request.getRequestURI().matches("/voucher-order/seckill/98[12][0-9]{2}");
            int cap = scaleFixture && rule.scope() != RateLimitScope.USER ? 5000 : rule.limit();
            buckets.add(new RateLimitBucket(key, cap, rule.windowSeconds() * 1000L));''')
        aspect.write_text(after, encoding='utf-8')
        self.prepare_source(work)
        write(self.path / 'isolated-limits.json', {
            'normal_global_ip_user': [300, 30, 5], 'scale_global_ip_user': [5000, 5000, 5],
            'only_scale_fixture_path_regex': '/voucher-order/seckill/98[12][0-9]{2}',
            'aspect_before_sha256': hashlib.sha256(before.encode()).hexdigest(),
            'aspect_after_sha256': hashlib.sha256(after.encode()).hexdigest(),
        })
        result = self.command([shutil.which('mvn.cmd'), '-o', '-B', '-f', work / 'pom.xml', '-Dmaven.test.skip=true', 'package'],
                              timeout=150, check=False, env=dict(self.env, MAVEN_OPTS='-Xms64m -Xmx512m'))
        (self.path / 'maven-scale.log').write_text(result.stdout + result.stderr, encoding='utf-8')
        if result.returncode:
            raise RuntimeError('Disposable JAR build failed')
        self.jar = work / 'target/localperks-backend-0.0.1-SNAPSHOT.jar'
        write(self.path / 'versions.json', {
            'jar_sha256': hashlib.sha256(self.jar.read_bytes()).hexdigest(),
            'normal_volumes_before': self.command(['docker', 'volume', 'ls', '--format', '{{.Name}}']).stdout.splitlines(),
            'jmx_sha256': hashlib.sha256(self.plan.read_bytes()).hexdigest(),
            'jmx': self.plan.name,
            'java': self.command(['java', '-version']).stderr,
        })

    def prepare_source(self, work):
        pass


async def sql_probe(run, voucher, stop, timeline):
    while not stop.is_set():
        try:
            result = await asyncio.to_thread(run.sql, f'SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id={voucher}')
            timeline.append((int(time.time() * 1000), int(result[0][0])))
        except Exception as error:
            timeline.append((int(time.time() * 1000), type(error).__name__))
        try:
            await asyncio.wait_for(stop.wait(), timeout=.25)
        except TimeoutError:
            pass


def summarize_http(rows):
    starts = [int(row['timeStamp']) for row in rows]
    elapsed = [int(row['elapsed']) for row in rows]
    # At the same millisecond include starts before ends. JTL resolution is 1 ms.
    points = sorted([(start, -1) for start in starts] +
                    [(start + length, 1) for start, length in zip(starts, elapsed)])
    running = peak = 0
    for _, sign in points:
        running -= sign
        peak = max(peak, running)
    first = min(starts)
    duration = max(start + length for start, length in zip(starts, elapsed)) - first
    responses = sum(len(row['responseCode']) == 3 and row['responseCode'].isdigit() for row in rows)
    return first, {
        'http_rps': len(rows) * 1000 / max(duration, 1), 'http_window_ms': duration,
        'http_responses': responses, 'http_response_rps': responses * 1000 / max(duration, 1),
        'jtl_max_overlapping_post': peak,
        'p50_ms': pct(elapsed, .50), 'p95_ms': pct(elapsed, .95), 'p99_ms': pct(elapsed, .99),
    }


async def wave(run, jmeter, profile, previous):
    group, users, loops, stock, voucher, expected = profile
    tag = f'{group}-{users}'
    run.stage(tag)
    run.sample('before-' + tag, guard=True)
    await reset_window(run)
    await run.coupon(voucher, stock)
    total = users * loops
    fixture = run.path / 'temp' / f'{tag}.csv'
    with fixture.open('w', encoding='utf-8', newline='') as stream:
        csv.writer(stream).writerows((uid, run.tokens[uid], '127.0.0.1') for uid in range(10001, 10001 + total))
    jtl = run.path / f'{tag}.jtl'
    dash = run.path / f'{tag}-dashboard'
    cmd = [jmeter, '-n', '-t', run.plan, '-l', jtl, '-j', run.path / f'{tag}-jmeter.log',
           '-e', '-o', dash, f'-Jport={run.java_port}', f'-Jvoucher={voucher}', f'-Jfixtures={fixture}',
           f'-Jthreads={users}', f'-Jloops={loops}', '-Jsample_variables=uid,outcome,orderId',
           '-Jjmeter.reportgenerator.overall_granularity=1000']
    if getattr(run, 'jmeter_force_exit', False):
        # Exit only after samples/report are complete; avoid unrelated non-daemon cleanup delaying audits.
        cmd.append('-Jjmeterengine.force.system.exit=true')
    env = dict(run.env, HEAP='-Xms96m -Xmx384m -XX:MaxMetaspaceSize=192m',
               JVM_ARGS=f'-Xss256k -Duser.home={run.path / "temp/jmeter-home"} ' + getattr(run, 'jmeter_vm_options', ''))
    stop = asyncio.Event()
    timeline = []
    probe = asyncio.create_task(sql_probe(run, voucher, stop, timeline))
    try:
        result = await asyncio.to_thread(run.command, cmd, timeout=getattr(run, 'load_timeout_seconds', 155), env=env, check=False)
        (run.path / f'{tag}-console.log').write_text(result.stdout + result.stderr, encoding='utf-8')
        with jtl.open(encoding='utf-8-sig', newline='') as stream:
            samples = list(csv.DictReader(stream))
        rows = [row for row in samples if row['label'].startswith('seckill.')]
        if not rows:
            raise RuntimeError('No measured samples: inspect warmup acceptance/drain failures in the JMeter log')
        preconnect = [row for row in samples if row['label'] == 'preconnect']
        accepted = [(row['orderId'], int(row['uid'])) for row in rows if row['outcome'] == 'accepted']
        first, http = summarize_http(rows)
        if getattr(run, 'open_arrivals', False):
            total = len(rows)
            expected = run.drain_target = len(accepted) if getattr(run, 'allow_backpressure', False) else total
        for _ in range(getattr(run, 'drain_probes', 40)):
            if any(isinstance(count, int) and count >= getattr(run, 'drain_target', len(accepted)) and stamp >= first
                   for stamp, count in timeline):
                break
            await asyncio.sleep(.5)
    finally:
        stop.set()
        await probe
        write(run.path / f'{tag}-sql-visibility.json', timeline)
    snapshot = await run.snapshot_voucher(voucher)
    db = {(str(row['id']), row['user_id']) for row in snapshot['orders']}
    order_count = len(db)
    terminal = next((stamp for stamp, count in timeline if stamp >= first and count == order_count), None)
    direct = getattr(run, 'direct_admission', False)
    offsets = await broker_progress(run, tag, previous + (len(accepted) if direct else len(snapshot['outbox'])))
    status_error = None
    try:
        status_count = await verify_states(run, accepted)
    except Exception as error:
        status_count, status_error = None, type(error).__name__
    count_429 = sum(row['responseCode'] == '429' for row in rows)
    sold_out = sum(row['outcome'] == 'sold_out' for row in rows)
    count_5xx = sum(row['responseCode'].isdigit() and 500 <= int(row['responseCode']) < 600 for row in rows)
    row = {'group': group, 'concurrency': users, 'loops': loops, 'attempted': len(rows), **http,
           'preconnect_samples': len(preconnect), 'preconnect_failures': sum(item['success'] != 'true' for item in preconnect),
           'http_429': count_429, 'http_5xx': count_5xx, 'sold_out': sold_out,
           'unknown_receipts': sum(item['outcome'] == 'unknown' for item in rows),
           'non_http_errors': sum(not (len(item['responseCode']) == 3 and item['responseCode'].isdigit()) for item in rows),
           'unexpected': len(rows) - len(accepted) - count_429 - sold_out, 'http_accepted': len(accepted),
           'mysql_orders': order_count, 'status_verified': status_count, 'status_error': status_error, 'initial_stock': stock,
           'redis_remaining': snapshot['redis_stock'], 'mysql_remaining': snapshot['mysql_stock'],
           'durable_orders_tps': round(order_count * 1000 / (terminal - first), 3) if terminal and terminal > first else None,
           'durable_observation_window_ms': terminal - first if terminal else None, 'mq': offsets,
           'outbox_sent': sum(item['status'] == 1 and bool(item['message_id']) for item in snapshot['outbox']),
           'admission_mode': 'broker-confirmed' if direct else 'sql-outbox',
           'admission_sql_rows': len(snapshot['outbox']),
           'order_ids_match': set(accepted) == db, 'qualified_users_match':
               set(snapshot['redis_qualified_users']) == {str(uid) for _, uid in db}, 'jmeter_exit': result.returncode,
           'dashboard': (dash / 'index.html').is_file()}
    row['pass'] = (len(rows) == len({item['uid'] for item in rows}) == total and row['unexpected'] == 0 and
                   count_429 == (total - expected if group == 'limit' or getattr(run, 'allow_backpressure', False) else 0) and
                   sold_out == (total - expected if group == 'oversell' else 0) and
                   order_count == len(accepted) == len({uid for _, uid in db}) == status_count == expected and
                   (row['admission_sql_rows'] == 0 if direct else row['outbox_sent'] == expected) and
                   row['redis_remaining'] == row['mysql_remaining'] == stock - expected >= 0 and
                   row['qualified_users_match'] and row['order_ids_match'] and offsets['converged'] and
                   terminal is not None and row['dashboard'] and result.returncode == 0)
    write(run.path / f'{tag}-audit.json', {'audit': row, 'final': snapshot})
    print(row, flush=True)
    return row, snapshot


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jmeter', type=Path, required=True)
    parser.add_argument('--warm-connections', action='store_true')
    parser.add_argument('--group', choices=('oversell', 'limit', 'throughput'), action='append')
    parser.add_argument('--threads', type=int, choices=(100, 200, 300, 500), action='append')
    args = parser.parse_args()
    jmeter = args.jmeter.resolve(strict=True)
    run = ScaleRun()
    run.plan = HERE / ('scale_seckill_warm.jmx' if args.warm_connections else 'scale_seckill.jmx')
    print('SCALE_JMETER ' + str(run.path), flush=True)
    results = []
    try:
        run.sample('before', guard=True)
        run.build()
        await run.infrastructure()
        await seed_users(run, 1000)
        run.monitor_thread = threading.Thread(target=run.monitor, daemon=True)
        run.monitor_thread.start()
        previous = 0
        for profile in PROFILES:
            if args.group and profile[0] != 'warmup' and profile[0] not in args.group:
                continue
            if args.threads and profile[0] != 'warmup' and profile[1] not in args.threads:
                continue
            try:
                row, snapshot = await wave(run, jmeter, profile, previous)
            except Exception as error:
                row = {'group': profile[0], 'concurrency': profile[1], 'pass': False,
                       'harness_error': type(error).__name__}
                results.append(row)
                write(run.path / 'scale-java-results.json', results)
                print(row, flush=True)
                continue
            results.append(row)
            write(run.path / 'scale-java-results.json', results)
            previous += len(snapshot['outbox'])
            if profile[0] == 'warmup' and row['pass']:
                run.replay_order_id = snapshot['orders'][0]['id']
                await run.replay()
                previous += 1
                proof = await broker_progress(run, 'duplicate-replay', previous)
                write(run.path / 'duplicate-replay-offsets.json', proof)
                assert proof['converged']
        if not all(row['pass'] for row in results):
            raise RuntimeError('At least one wave failed; see scale-java-results.json')
    except BaseException as exc:
        run.failure = f'{type(exc).__name__}: {exc}'
        write(run.path / 'scale-java-failure.json', {'error': run.failure})
        raise
    finally:
        run.cleanup()


if __name__ == '__main__':
    asyncio.run(main())
