"""Run the JMeter plan against a new LocalPerks stack with synthetic accounts."""
import argparse
import asyncio
import csv
import threading
from pathlib import Path

import httpx

from run_k6 import Run, broker_progress, reset_window, write
from run import pct

HERE = Path(__file__).resolve().parent
TOTAL = 80


async def verify_states(run, accepted):
    limit = asyncio.Semaphore(8)
    async with httpx.AsyncClient(base_url=run.java_url, timeout=5, trust_env=False) as client:
        async def read_status(order_id, uid):
            async with limit:
                response = await client.get(f'/voucher-order/status/{order_id}',
                                            headers={'authorization': run.tokens[uid]})
                payload = response.json()
                return response.status_code == 200 and payload['success'] and payload['data']['statusCode'] == 12

        return sum(await asyncio.gather(*(read_status(order, uid) for order, uid in accepted)))


async def phase(run, jmeter, name, voucher, expected, previous, distributed):
    run.stage('jmeter-' + name)
    run.sample('before-jmeter-' + name, guard=True)
    await reset_window(run)
    stock = TOTAL + 5
    await run.coupon(voucher, stock)
    fixture = run.path / 'temp' / f'{name}.csv'
    with fixture.open('w', encoding='utf-8', newline='') as stream:
        csv.writer(stream).writerows(
            (uid, run.tokens[uid], f'198.18.0.{index + 1}' if distributed else '127.0.0.1')
            for index, uid in enumerate(range(10001, 10001 + TOTAL))
        )
    jtl = run.path / f'{name}.jtl'
    dashboard = run.path / f'{name}-dashboard'
    env = dict(run.env, HEAP='-Xms96m -Xmx384m -XX:MaxMetaspaceSize=192m',
               JVM_ARGS=f'-Duser.home={run.path / "temp/jmeter-home"}')
    result = run.command([
        jmeter, '-n', '-t', HERE / 'seckill.jmx', '-l', jtl, '-j', run.path / f'{name}-jmeter.log',
        '-e', '-o', dashboard, f'-Jport={run.java_port}', f'-Jvoucher={voucher}', f'-Jfixtures={fixture}',
        '-Jsample_variables=uid,outcome,orderId', '-Jjmeter.reportgenerator.overall_granularity=1000',
    ], timeout=90, env=env, check=False)
    (run.path / f'{name}-console.log').write_text(result.stdout + result.stderr, encoding='utf-8')
    with jtl.open(encoding='utf-8-sig', newline='') as stream:
        rows = list(csv.DictReader(stream))
    accepted = [row for row in rows if row['outcome'] == 'accepted']
    rejected = [row for row in rows if row['outcome'] == 'rate_limited']
    observed = {(row['orderId'], int(row['uid'])) for row in accepted}
    elapsed = [int(row['elapsed']) for row in accepted]
    accepted_count = len(accepted)
    snapshot = await run.wait_orders(voucher, accepted_count)
    db_orders = {(str(row['id']), row['user_id']) for row in snapshot['orders']}
    visible = await verify_states(run, observed)
    mq_offsets = await broker_progress(run, name, previous + accepted_count)
    stock_matches = snapshot['redis_stock'] == snapshot['mysql_stock'] == stock - accepted_count
    qualified_matches = set(snapshot['redis_qualified_users']) == {str(uid) for _, uid in db_orders}
    start = min(int(row['timeStamp']) for row in rows)
    finish = max(int(row['timeStamp']) + int(row['elapsed']) for row in rows)
    duration_s = (finish - start) / 1000
    audit = {
        'phase': name, 'target_rps': 10, 'sample_duration_s': duration_s,
        'observed_rps': len(rows) / duration_s, 'http_attempts': len(rows), 'http_429': len(rejected),
        'unexpected': len(rows) - accepted_count - len(rejected), 'http_accepted': accepted_count,
        'http_accepted_p95_ms': pct(elapsed, .95), 'http_accepted_p99_ms': pct(elapsed, .99),
        'mysql_orders': len(db_orders), 'status_verified_after_test': visible,
        'order_ids_and_owners_match': observed == db_orders, 'inventory_matches': stock_matches,
        'outbox_sent': sum(row['status'] == 1 and bool(row['message_id']) for row in snapshot['outbox']),
        'redis_qualification_matches': qualified_matches, 'mq_offsets': mq_offsets,
        'html_dashboard_created': (dashboard / 'index.html').is_file(), 'jmeter_exit': result.returncode,
    }
    write(run.path / f'{name}-audit.json', {'audit': audit, 'final_state': snapshot})
    print(audit, flush=True)
    if not (len(rows) == len({row['uid'] for row in rows}) == TOTAL and
            accepted_count == len(observed) == len(db_orders) == visible == expected and
            len(rejected) == TOTAL - expected and observed == db_orders and stock_matches and
            qualified_matches and audit['outbox_sent'] == expected and mq_offsets['converged'] and
            audit['html_dashboard_created'] and result.returncode == 0):
        raise RuntimeError(f'JMeter reconciliation failed for {name}')
    return audit


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jmeter', type=Path, required=True, help='Path to Apache JMeter bin/jmeter.bat')
    args = parser.parse_args()
    jmeter = args.jmeter.resolve(strict=True)
    run = Run()
    print(f'SHOPLINK_JMETER_RUN {run.path}', flush=True)
    run.env['HMDP_RATE_LIMIT_TRUST_FORWARDED_HEADERS'] = 'true'
    try:
        run.sample('before', guard=True)
        run.build()
        await run.infrastructure()
        run.monitor_thread = threading.Thread(target=run.monitor, daemon=True)
        run.monitor_thread.start()
        audits = []
        for index, (name, expected, distributed) in enumerate((('same-ip', 30, False), ('distributed', 80, True))):
            previous = sum(audit['http_accepted'] for audit in audits)
            audits.append(await phase(run, jmeter, name, 97000 + index, expected, previous, distributed))
            write(run.path / 'jmeter-results.json', audits)
    except BaseException as exc:
        run.failure = f'{type(exc).__name__}: {exc}'
        write(run.path / 'jmeter-failure.json', {'error': run.failure})
        raise
    finally:
        run.cleanup()


if __name__ == '__main__':
    asyncio.run(main())
