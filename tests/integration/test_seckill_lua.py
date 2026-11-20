"""Actual production Lua, only on the run's disposable Redis; no Java/MQ claims."""
import asyncio
import csv
import json
import os
import time
from pathlib import Path
from urllib.parse import urlparse

import pytest


def test_real_lua_atomic_stock_and_one_user_one_reservation():
    url = os.getenv('AGENT_TEST_REDIS_URL')
    if not url:
        pytest.skip('Dedicated run Redis required')
    endpoint = urlparse(url)
    assert endpoint.hostname == '127.0.0.1' and endpoint.port != 6379
    run = Path(os.environ['VALIDATION_RUN_DIR'])
    manifest = json.loads((run / 'resource_manifest.json').read_text(encoding='utf-8-sig'))
    assert any(r['type'] == 'container' and r['isolated'] and r['run_id'] == run.name for r in manifest['resources'])
    script = (Path(__file__).resolve().parents[2] / 'src/main/resources/seckill.lua').read_text(encoding='utf-8')

    async def check():
        from redis.asyncio import Redis
        rows = []
        async with Redis.from_url(url, decode_responses=True, max_connections=100) as redis:
            for concurrency in (20, 50, 100):
                voucher = f'{run.name}:lua:{concurrency}'
                stock_key, users_key = f'seckill:stock:{voucher}', f'seckill:order:{voucher}'
                initial = concurrency // 2
                try:
                    await redis.set(stock_key, initial)
                    await redis.hset(f'seckill:activity:{voucher}', mapping={'ready':'1','beginAt':'1','endAt':'9999999999999'})
                    await redis.hset(f'seckill:purchased:{voucher}', '__ready__', '1')
                    started = time.perf_counter()
                    replies = await asyncio.gather(*(redis.eval(script, 0, voucher, str(i)) for i in range(concurrency)))
                    elapsed = time.perf_counter() - started
                    remaining, cardinality = int(await redis.get(stock_key)), await redis.scard(users_key)
                    assert replies.count(0) == initial and remaining == 0 and cardinality == initial
                    assert replies.count(1) == concurrency - initial
                    rows.append({'component': 'actual_seckill_lua_isolated_redis', 'concurrency': concurrency,
                                 'requests': len(replies), 'successful_reservations': replies.count(0),
                                 'initial_stock': initial, 'remaining_stock': remaining, 'unique_users': cardinality,
                                 'duration_seconds': elapsed, 'rps': concurrency / elapsed, 'mysql_orders': 'NOT_VERIFIED', 'mq_delivery': 'NOT_VERIFIED'})
                finally:
                    await redis.delete(stock_key, users_key, f'seckill:activity:{voucher}', f'seckill:purchased:{voucher}')
            voucher = run.name + ':lua:dedupe'
            stock_key, users_key = f'seckill:stock:{voucher}', f'seckill:order:{voucher}'
            try:
                await redis.set(stock_key, 2)
                await redis.hset(f'seckill:activity:{voucher}', mapping={'ready':'1','beginAt':'1','endAt':'9999999999999'})
                await redis.hset(f'seckill:purchased:{voucher}', '__ready__', '1')
                assert await redis.eval(script, 0, voucher, 'same-user') == 0
                assert await redis.eval(script, 0, voucher, 'same-user') == 2
                assert int(await redis.get(stock_key)) == 1
                assert await redis.scard(users_key) == 1
            finally:
                await redis.delete(stock_key, users_key, f'seckill:activity:{voucher}', f'seckill:purchased:{voucher}')
        with (run / 'seckill_component.csv').open('w', encoding='utf-8-sig', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    asyncio.run(check())
