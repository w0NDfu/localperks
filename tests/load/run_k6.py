"""Run k6 against a freshly created, manifest-owned LocalPerks stack."""
import argparse
import asyncio
import json
import sys
import threading
from pathlib import Path

from redis.asyncio import Redis

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'real_e2e'))
from run import Run, write

# Tuple fields are phase name, offers per second, duration, IP distribution and expected accepted.
PROFILES = (
    ('warmup', 4, 2, False, 8),
    ('same-ip', 10, 8, False, 30),
    ('distributed-burst', 20, 12, True, 240),
    ('distributed-steady', 4, 75, True, 300),
)


async def seed_users(run, total):
    extras = range(10101, 10001 + total)
    users = ','.join(f"({uid},'198{uid:08d}','Load fixture')" for uid in extras)
    if users:
        run.sql('INSERT INTO tb_user(id,phone,nick_name) VALUES' + users + ';')
    async with Redis.from_url(run.redis_url) as redis:
        async with redis.pipeline(transaction=False) as pipe:
            for uid in extras:
                token = f'{run.id}-load-user-{uid}'
                run.tokens[uid] = token
                pipe.hset('login:token:' + token, mapping={'id': uid, 'nickName': 'Load fixture'})
                pipe.expire('login:token:' + token, 3600)
            await pipe.execute()


async def reset_window(run):
    # Exact known test keys, with no effect on the normal Redis instance.
    prefix = 'rate:sliding:{voucher-seckill}:'
    keys = [prefix + 'global:all', prefix + 'ip:127.0.0.1']
    keys.extend(prefix + 'user:' + str(uid) for uid in run.tokens)
    keys.extend(prefix + f'ip:198.18.{i // 254}.{i % 254 + 1}' for i in range(len(run.tokens)))
    async with Redis.from_url(run.redis_url) as redis:
        await redis.delete(*keys)


def metric(metrics, name, key='count'):
    return metrics.get(name, {}).get('values', {}).get(key, 0)


async def phase(run, k6, index, profile):
    name, rate, seconds, distributed, expected = profile
    total = rate * seconds
    run.sample('before-k6-' + name, guard=True)
    run.stage('k6-' + name)
    await reset_window(run)
    voucher = 96000 + index
    initial_stock = total + 5
    await run.coupon(voucher, initial_stock)
    fixture = run.path / 'temp' / 'k6-fixture.json'
    summary = run.path / f'{name}-k6.json'
    orders_path = run.path / f'{name}-http-orders.jsonl'
    write(fixture, {
        'baseUrl': run.java_url, 'voucher': voucher, 'rate': rate, 'seconds': seconds,
        'distributed': distributed, 'tokens': [run.tokens[uid] for uid in range(10001, 10001 + total)],
        'vus': 32 if rate >= 10 else 12,
    })
    env = dict(run.env, SHOPLINK_K6_FIXTURE=str(fixture), SHOPLINK_K6_SUMMARY=str(summary),
               K6_NO_USAGE_REPORT='true', NO_PROXY='127.0.0.1')
    result = run.command([k6, 'run', '--quiet', '--log-format', 'raw', '--console-output', orders_path,
                          HERE / 'seckill.k6.js'], timeout=seconds + 45, check=False, env=env)
    (run.path / f'{name}-k6.log').write_text(result.stdout + result.stderr, encoding='utf-8')
    metrics = json.loads(summary.read_text(encoding='utf-8'))['metrics']
    attempted = int(metric(metrics, 'iterations'))
    dropped = int(metric(metrics, 'dropped_iterations'))
    accept_count = int(metric(metrics, 'seckill_accepted'))
    reject_count = int(metric(metrics, 'seckill_429'))
    visible = int(metric(metrics, 'seckill_finalized'))
    rows = [json.loads(line) for line in orders_path.read_text(encoding='utf-8').splitlines() if line.strip()]
    snapshot = await run.wait_orders(voucher, accept_count)
    mq_offsets = await broker_progress(run, name, sum(item[4] for item in PROFILES[:index]) + accept_count)
    http_orders = {(row['orderId'], 10001 + row['index']) for row in rows if row.get('state') == 12}
    db_orders = {(str(row['id']), row['user_id']) for row in snapshot['orders']}
    stock_matches = snapshot['mysql_stock'] == snapshot['redis_stock'] == initial_stock - accept_count
    audit = {
        'phase': name, 'rate': rate, 'seconds': seconds, 'scheduled': total,
        'attempted': attempted, 'dropped': dropped, 'accepted': accept_count, 'http_429': reject_count,
        'unexpected': int(metric(metrics, 'seckill_unexpected')),
        'status_reads': int(metric(metrics, 'seckill_status_reads')),
        'status_failures': int(metric(metrics, 'seckill_status_failures')),
        'visible_orders': visible, 'final_mysql_orders': len(db_orders),
        'k6_exit': result.returncode, 'submit_accepted_p95_ms': metric(metrics, 'seckill_accept_ms', 'p(95)'),
        'http_429_p95_ms': metric(metrics, 'seckill_reject_ms', 'p(95)') if reject_count else None,
        'submit_to_visible_p95_ms': metric(metrics, 'seckill_order_visible_ms', 'p(95)'),
        'submit_to_visible_p99_ms': metric(metrics, 'seckill_order_visible_ms', 'p(99)'),
        'order_ids_and_owners_match': http_orders == db_orders,
        'inventory_matches': stock_matches,
        'redis_qualification_matches': set(snapshot['redis_qualified_users']) == {str(uid) for _, uid in db_orders},
        'outbox_sent': sum(row['status'] == 1 and bool(row['message_id']) for row in snapshot['outbox']),
        'mq_offsets': mq_offsets,
    }
    write(run.path / f'{name}-audit.json', {'audit': audit, 'final_state': snapshot})
    print(json.dumps(audit), flush=True)
    if not (attempted == total and dropped == 0 and result.returncode == 0 and
            accept_count == visible == len(db_orders) == len({uid for _, uid in db_orders}) == expected and
            reject_count == total - expected and audit['order_ids_and_owners_match'] and stock_matches and
            audit['redis_qualification_matches'] and audit['outbox_sent'] == expected and mq_offsets['converged']):
        raise RuntimeError(f'Load or reconciliation assertion failed for {name}')
    return audit


async def broker_progress(run, name, expected):
    for attempt in range(1, 6):
        result = run.admin(['consumerProgress', '-n', 'namesrv:9876', '-g', 'hmdp-seckill-order-consumer'],
                           f'{name}-mq-progress-{attempt}')
        queues = [line.split() for line in result.stdout.splitlines() if line.startswith('seckill-order-topic ')]
        totals = [sum(int(queue[column]) for queue in queues) for column in range(3, 7)]
        converged = result.returncode == 0 and bool(queues) and totals == [expected, expected, 0, 0]
        if converged:
            break
        await asyncio.sleep(1)
    return dict(zip(('broker_offset', 'consumer_offset', 'lag', 'inflight'), totals), converged=converged)


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--k6', type=Path, required=True)
    parser.add_argument('--smoke', action='store_true', help='Run only the eight-request warmup profile')
    args = parser.parse_args()
    k6 = args.k6.resolve(strict=True)
    run = Run()
    print(f'SHOPLINK_K6_RUN {run.path}', flush=True)
    # Loopback-only test Java honors synthetic client IPs, retaining real limiter thresholds.
    run.env['HMDP_RATE_LIMIT_TRUST_FORWARDED_HEADERS'] = 'true'
    profiles = PROFILES[:1] if args.smoke else PROFILES
    try:
        run.sample('before', guard=True)
        run.build()
        version = run.command([k6, 'version']).stdout.strip()
        write(run.path / 'k6-setup.json', {'version': version, 'profiles': profiles, 'model_requests': 0})
        await run.infrastructure()
        await seed_users(run, max(rate * seconds for _, rate, seconds, _, _ in profiles))
        run.monitor_thread = threading.Thread(target=run.monitor, daemon=True)
        run.monitor_thread.start()
        rows = []
        for index, profile in enumerate(profiles):
            rows.append(await phase(run, k6, index, profile))
            write(run.path / 'k6-results.json', rows)
    except BaseException as exc:
        run.failure = f'{type(exc).__name__}: {exc}'
        write(run.path / 'k6-failure.json', {'error': run.failure})
        raise
    finally:
        run.cleanup()


if __name__ == '__main__':
    asyncio.run(main())
