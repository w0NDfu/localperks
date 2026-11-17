"""Seed and independently capture new scale holdouts in a real isolated Java stack."""
import asyncio
import time
import zipfile

import httpx
from redis.asyncio import Redis

from ops.tools import LiveProvider, ObservationScope, TOOLS

def now_ms():
    return int(time.time() * 1000)


async def seed_consumer(run, cases):
    shops = [f"({item['shop']},'{item['name']}',1,'验收合成地址',{item['x']},{item['y']},"
             f"{item['avg']},'10:00-{item['close']:02d}:00')"
             for case in cases for item in case['facts']]
    run.sql('INSERT INTO tb_shop(id,name,type_id,address,x,y,avg_price,open_hours) VALUES' + ','.join(shops) + ';')
    vouchers = []
    stocks = []
    async with Redis.from_url(run.redis_url) as redis:
        async with redis.pipeline(transaction=False) as pipe:
            for case in cases:
                for item in case['facts']:
                    voucher, shop, pay, rules = item['voucher'], item['shop'], item['pay'], item['rules']
                    vouchers.append(f"({voucher},{shop},'隔离验收券{voucher}','{rules}',{pay},10000,1,1)")
                    days = 1 if not item['active'] else -1
                    stocks.append(f"({voucher},{item['stock']},DATE_ADD(NOW(), INTERVAL {days} DAY),DATE_ADD(NOW(), INTERVAL 7 DAY))")
                    pipe.geoadd('shop:geo:1', [item['x'], item['y'], str(shop)])
                    pipe.set(f'seckill:stock:{voucher}', item['stock'])
            await pipe.execute()
    run.sql('INSERT INTO tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) VALUES' + ','.join(vouchers) +
            '; INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) VALUES' + ','.join(stocks) + ';')
    return {
        'shops': run.sql_json("SELECT JSON_OBJECT('id',id,'name',name,'avg',avg_price,'x',x,'y',y,'hours',open_hours) FROM tb_shop WHERE id>=31000"),
        'vouchers': run.sql_json("SELECT JSON_OBJECT('id',v.id,'shop',v.shop_id,'pay',v.pay_value,'face',v.actual_value,'rules',v.rules,'stock',s.stock,'active',IF(s.begin_time<=NOW(),1,0)) FROM tb_voucher v JOIN tb_seckill_voucher s ON v.id=s.voucher_id WHERE v.id>=41000"),
    }


class FaultCapture:
    def __init__(self, run, token):
        self.run, self.token = run, token
        self.classpath = None

    async def submit(self, voucher, uid, ip='127.0.0.1'):
        async with httpx.AsyncClient(base_url=self.run.java_url, timeout=10, trust_env=False) as client:
            response = await client.post(f'/voucher-order/seckill/{voucher}',
                                         headers={'authorization': self.run.tokens[uid], 'X-Forwarded-For': ip})
            return {'http': response.status_code, 'body': response.json()}

    def consume(self, enabled, case):
        result = self.run.admin(['updateSubGroup', '-n', 'namesrv:9876', '-b', f'127.0.0.1:{self.run.broker_port}',
                                 '-g', 'hmdp-seckill-order-consumer', '-s', str(enabled).lower()],
                                f'{case}-consume-{enabled}')
        if result.returncode != 0:
            raise RuntimeError('Isolated consumer setting did not change')

    def replay(self, order):
        run = self.run
        if not self.classpath:
            libs = run.path / 'temp/java-libs'
            libs.mkdir()
            with zipfile.ZipFile(run.jar) as archive:
                for name in archive.namelist():
                    if name.startswith('BOOT-INF/lib/') and name.endswith('.jar'):
                        (libs / name.rsplit('/', 1)[1]).write_bytes(archive.read(name))
            self.classpath = str(run.path / 'temp') + ';' + str(libs / '*')
            run.command(['javac', '-encoding', 'UTF-8', '-cp', self.classpath, '-d', run.path / 'temp',
                         run.path.parents[2] / 'tests/real_e2e/ReplayOrder.java'])
        stored = run.sql(f"SELECT message_body FROM tb_mq_message WHERE order_id={int(order)} AND topic='seckill-order-topic'")
        if stored:
            body = stored[0][0]
        else:
            import json
            rows = run.sql(f"SELECT id,user_id,voucher_id FROM tb_voucher_order WHERE id={int(order)}")
            if not rows:
                raise RuntimeError('Cannot replay an unknown order')
            body = json.dumps(dict(zip(('orderId', 'userId', 'voucherId'), map(int, rows[0]))))
        return run.command(['java', '-Xmx128m', '-Duser.home=' + str(run.path / 'temp/java-home'),
                            '-Drocketmq.client.logRoot=' + str(run.path / 'temp/mq-client-logs'), '-cp', self.classpath,
                            'ReplayOrder', f'127.0.0.1:{run.namesrv_port}', 'seckill-order-topic', body, run.id],
                           timeout=40).stdout

    async def capture(self, case):
        run, family, voucher = self.run, case['family'], case['voucher']
        await run.clear_stage_window()
        await run.coupon(voucher, 0 if family == 'ip-limit' else case['stock'])
        start = now_ms()
        ip = f"198.19.0.{case['replica'] + 1}" if family == 'ip-limit' else '127.0.0.1'
        inputs = []
        if family == 'mq-backlog':
            self.consume(False, case['id'])
        try:
            async with Redis.from_url(run.redis_url) as redis:
                if family == 'lower-stock':
                    await redis.set(f'seckill:stock:{voucher}', case['stock'] - case['delta'])
                elif family == 'higher-stock':
                    await redis.set(f'seckill:stock:{voucher}', case['stock'] + case['delta'])
                elif family == 'cache-missing':
                    await redis.delete(f'seckill:stock:{voucher}')
                elif family == 'historical':
                    start -= (2 + case['replica']) * 60000
                elif family == 'ip-limit':
                    async with httpx.AsyncClient(base_url=run.java_url, timeout=10, trust_env=False) as client:
                        for index in range(31):
                            response = await client.post(f'/voucher-order/seckill/{voucher}', headers={
                                'authorization': run.tokens[10001 + index], 'X-Forwarded-For': ip})
                            inputs.append(response.status_code)
                    if inputs.count(429) != 1:
                        raise RuntimeError('Isolated IP fixture did not emit exactly one rejection')
                elif family in ('duplicate', 'mq-backlog'):
                    inputs = [await self.submit(voucher, 10021 + index) for index in range(
                        1 if family == 'duplicate' else case['events'])]
                    if not all(item['body']['success'] for item in inputs):
                        raise RuntimeError('Fault fixture was not accepted by Java')
                    if family == 'duplicate':
                        await run.wait_orders(voucher, 1)
                        for _ in range(1 + case['replica'] % 2):
                            sent = self.replay(inputs[0]['body']['data']['orderId'])
                            if 'VALIDATION_SEND SEND_OK' not in sent:
                                raise RuntimeError('Replay was not accepted by Broker')
                    await asyncio.sleep(2.5)
            scope = ObservationScope(start=start, end=max(now_ms(), start + 1) if family != 'historical' else start + 1000,
                                     voucher_id=voucher, ip=ip)
            async with httpx.AsyncClient(base_url=run.java_url, timeout=8, trust_env=False) as client:
                provider = LiveProvider(client, self.token)
                samples = {tool: [await provider.read(tool, scope)] for tool in TOOLS}
                await asyncio.sleep(.4)
                samples['inventory'].append(await provider.read('inventory', scope))
            snapshot = {'scope': scope.model_dump(), 'samples': samples, 'injection_requests': inputs,
                        'origin': 'Dedicated real Java read-only HTTP observations'}
            if not proof(case, [sample for observations in samples.values() for sample in observations]):
                raise RuntimeError('Ground truth did not match independently captured observations')
            return snapshot
        finally:
            if family == 'mq-backlog':
                self.consume(True, case['id'])
                await run.wait_orders(voucher, case['events'])


def proof(case, observations):
    family = case['family']
    data = {item['tool']: item['data'] for item in observations}
    if family in ('lower-stock', 'higher-stock'):
        pairs = [(int(item['data']['mysql'][0]['stock']), int(item['data']['redisStock']),
                  item['data']['pending'][0]['total'], item['observedAt'])
                 for item in observations if item['tool'] == 'inventory']
        sign = -1 if family == 'lower-stock' else 1
        expected = (case['stock'], case['stock'] + sign * case['delta'], 0)
        return len(pairs) == 2 and all(pair[:3] == expected for pair in pairs) and pairs[1][3] - pairs[0][3] >= 300
    if family == 'cache-missing':
        return 'inventory' in data and data['inventory'].get('redisStock') is None
    if family == 'historical':
        return any(item['observedAt'] - item['windowEnd'] > 30000 for item in observations)
    if family == 'ip-limit':
        return any(event.get('status') == 429 and event.get('limitScope') == 'ip'
                   for item in data.values() for event in item.get('events', []))
    if family == 'mq-backlog':
        mq = data.get('mq', {})
        return (mq.get('consumeEnabled') is False and mq.get('lag', 0) >= case['events'] and
                data.get('inventory', {}).get('pending', [{}])[0].get('total') == case['events'])
    if family == 'duplicate':
        delivery = data.get('delivery', {})
        orders = delivery.get('orders', [])
        events = delivery.get('events', [])
        if len(orders) != 1:
            return False
        order = str(orders[0]['orderId'])
        return (sum(event['type'] == 'mq_received' and str(event.get('orderId')) == order for event in events) >= 2 and
                any(event['type'] == 'order_idempotent' and str(event.get('orderId')) == order for event in events))
    return False
