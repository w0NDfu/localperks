"""Eight pre-labeled fault injections into a fresh, manifest-owned stack."""
import asyncio
import hashlib
import importlib.util
import json
import secrets
import threading
import time
import traceback
import uuid
import zipfile
from datetime import datetime
from pathlib import Path

import httpx
from redis.asyncio import Redis

spec = importlib.util.spec_from_file_location('p4_capture', Path(__file__).with_name('run.py'))
p4_capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p4_capture)
P4Run, ROOT, write = p4_capture.P4Run, p4_capture.ROOT, p4_capture.write
from ops.tools import LiveProvider, ObservationScope, TOOLS


UNKNOWN = 'INSUFFICIENT_EVIDENCE'
CASES = {
    'ip-and-drift': (95101, UNKNOWN, '这张券同时有人遇到 429 和库存偏差。两类现象并存，请调查是否有唯一主因。'),
    'queue-and-residual': (95102, 'MQ_CONSUME_DISABLED', '请求受理后迟迟未落库。请核对消费组，并解释库存和在途预留的矛盾。'),
    'changing-stock': (95103, UNKNOWN, '有人报库存不一致，请核对两次库存采样并确认差额是否持续。'),
    'missing-cache-key': (95104, UNKNOWN, '库存查询缺一侧结果。请确认能否判断持续库存偏移。'),
    'historical-stock': (95105, UNKNOWN, '复盘指定历史窗口中的库存异常，核对快照与事件的先后。'),
    'user-dimension': (95106, UNKNOWN, '用户连续请求后收到 429，请核对拒绝维度，确定是否是出口限流。'),
    'partial-redelivery': (95107, UNKNOWN, '这个窗口内有重复处理的线索，请核对是否能证实发生了两次投递。'),
    'compensation-and-drift': (95108, 'REDIS_STOCK_DRIFT', '订单出现预留补偿。请按时序核对补偿后两侧库存是否还有差额。'),
}


def now():
    return int(time.time() * 1000)


class Capture(P4Run):
    async def stock(self, voucher, value):
        async with Redis.from_url(self.redis_url, decode_responses=True) as redis:
            key = f'seckill:stock:{voucher}'
            if value is None:
                await redis.delete(key)
            else:
                await redis.set(key, value)

    async def snapshot(self, name, start, *, end=None, between=None):
        voucher = CASES[name][0]
        scope = ObservationScope(start=start, end=end or now(), voucher_id=voucher)
        async with httpx.AsyncClient(base_url=self.java_url, timeout=8, trust_env=False) as client:
            provider = LiveProvider(client, self.ops_java)
            samples = {tool: [await provider.read(tool, scope)] for tool in TOOLS}
            if between is not None:
                await between()
            await asyncio.sleep(.4)
            samples['inventory'].append(await provider.read('inventory', scope))
        raw = {'scope': scope.model_dump(), 'samples': samples, 'origin': 'Independent real Java HTTP observations in isolated Docker'}
        write(self.path / f'snapshot-{name}.json', raw)
        self.stage(f'captured-{name}')
        return samples

    async def redeliver(self, order_id):
        libs = self.path / 'temp/java-libs'
        libs.mkdir()
        with zipfile.ZipFile(self.jar) as archive:
            for member in archive.namelist():
                if member.startswith('BOOT-INF/lib/') and member.endswith('.jar'):
                    (libs / member.rsplit('/', 1)[1]).write_bytes(archive.read(member))
        classpath = str(ROOT / 'target/classes') + ';' + str(libs / '*')
        self.command(['javac', '-encoding', 'UTF-8', '-cp', classpath, '-d', self.path / 'temp', ROOT / 'tests/real_e2e/ReplayOrder.java'])
        body = self.sql(f"SELECT message_body FROM tb_mq_message WHERE order_id={int(order_id)} AND topic='seckill-order-topic';")[0][0]
        sent = self.command(['java', '-Xmx128m', '-XX:MaxDirectMemorySize=64m', '-Dfile.encoding=UTF-8',
            '-Duser.home=' + str(self.path / 'temp/java-home'), '-Drocketmq.client.logRoot=' + str(self.path / 'temp/mq-client-logs'),
            '-cp', str(self.path / 'temp') + ';' + classpath, 'ReplayOrder', f'127.0.0.1:{self.namesrv_port}',
            'seckill-order-topic', body, self.id], timeout=40)
        (self.path / 'partial-redelivery-producer.log').write_text(sent.stdout + sent.stderr, encoding='utf-8')
        assert 'VALIDATION_SEND SEND_OK' in sent.stdout

    def group(self, enabled):
        result = self.admin(['updateSubGroup', '-n', 'namesrv:9876', '-b', f'127.0.0.1:{self.broker_port}',
            '-g', 'hmdp-seckill-order-consumer', '-s', str(enabled).lower()], f'p41-group-{enabled}')
        assert result.returncode == 0

    async def capture_cases(self):
        # Normal order registers actual offsets and provides a distinct live API smoke scope.
        await self.coupon(95100, 10)
        warm_start = now()
        assert (await self.submit(95100, 10001))['body']['success']
        await self.wait_orders(95100, 1)
        await asyncio.sleep(6)
        async with httpx.AsyncClient(timeout=12, trust_env=False) as client:
            response = await client.post(self.agent_url + '/agent/ops/investigations', headers={'X-Ops-Token': self.ops_admin},
                json={'symptom': '核对当前券的业务运行状态', 'scope': {'start': warm_start, 'end': now(), 'voucher_id': 95100}})
        warm = response.json()
        write(self.path / 'live-fastpath.json', warm)
        assert warm['root_code'] == 'NO_FAULT' and warm['route']['path'] == 'rules_fastpath' and not warm['model_usage']

        # Record a real prior window before performing an unrelated later write.
        await self.coupon(95105, 13)
        historical_start = now()
        async with httpx.AsyncClient(base_url=self.java_url, timeout=8, trust_env=False) as client:
            await asyncio.sleep(.03)
            historical_end = now()
            prior_scope = ObservationScope(start=historical_start, end=historical_end, voucher_id=95105)
            historical_before = await LiveProvider(client, self.ops_java).read('inventory', prior_scope)
        assert historical_before['data']['redisStock'] == '13'
        write(self.path / 'historical-stock-before.json', historical_before)

        await self.coupon(95101, 100, future=True)
        await self.clear_stage_window()
        start = now()
        replies = [await self.submit(95101, 10001 + i) for i in range(31)]
        assert replies[-1]['http'] == 429 and all(reply['http'] == 200 for reply in replies[:-1])
        await self.stock(95101, 94)
        samples = await self.snapshot('ip-and-drift', start)
        assert any(e.get('limitScope') == 'ip' for e in samples['events'][0]['data']['events'])
        assert all(x['data']['redisStock'] == '94' for x in samples['inventory'])
        write(self.path / 'ip-and-drift-injection.json', {'requests': replies, 'redis_written': 94, 'mysql_initial': 100})

        await self.coupon(95102, 8, mysql_stock=12)
        self.group(False)
        try:
            await self.clear_stage_window()
            start = now()
            submitted = [await self.submit(95102, user) for user in (10002, 10003)]
            assert all(reply['body']['success'] for reply in submitted)
            await asyncio.sleep(3)
            raw = await self.snapshot('queue-and-residual', start)
            assert raw['mq'][0]['data']['consumeEnabled'] is False and raw['mq'][0]['data']['lag'] == 2
            stock = raw['inventory'][0]['data']
            assert stock['pending'][0]['total'] == 2 and stock['redisStock'] == '6' and stock['mysql'][0]['stock'] == 12
            write(self.path / 'queue-and-residual-injection.json', {'submitted': submitted, 'before_reservation': {'mysql': 12, 'redis': 8}})
        finally:
            self.group(True)
        await self.wait_orders(95102, 2)

        await self.coupon(95103, 19)
        start = now() - 5
        await self.stock(95103, 14)
        samples = await self.snapshot('changing-stock', start, between=lambda: self.stock(95103, 19))
        assert [sample['data']['redisStock'] for sample in samples['inventory']] == ['14', '19']
        write(self.path / 'changing-stock-injection.json', {'writes': [14, 19], 'mysql_initial': 19})

        await self.coupon(95104, 17)
        start = now() - 5
        await self.stock(95104, None)
        samples = await self.snapshot('missing-cache-key', start)
        assert all(sample['data'].get('redisStock') is None for sample in samples['inventory'])
        write(self.path / 'missing-cache-key-injection.json', {'deleted_key': 'seckill:stock:95104', 'mysql_initial': 17})

        await self.coupon(95106, 25, future=True)
        await self.clear_stage_window()
        start = now()
        replies = [await self.submit(95106, 10004) for _ in range(6)]
        assert replies[-1]['http'] == 429 and all(reply['http'] == 200 for reply in replies[:-1])
        samples = await self.snapshot('user-dimension', start)
        assert any(e.get('limitScope') == 'user' for e in samples['events'][0]['data']['events'])
        write(self.path / 'user-dimension-injection.json', {'requests': replies})

        await self.coupon(95107, 9)
        await self.clear_stage_window()
        full_start = now()
        reply = await self.submit(95107, 10005)
        assert reply['body']['success']
        await self.wait_orders(95107, 1)
        partial_start = now()
        await self.redeliver(reply['body']['data']['orderId'])
        await asyncio.sleep(3)
        samples = await self.snapshot('partial-redelivery', partial_start)
        scope = ObservationScope(start=full_start, end=now(), voucher_id=95107)
        async with httpx.AsyncClient(base_url=self.java_url, timeout=8, trust_env=False) as client:
            full = await LiveProvider(client, self.ops_java).read('delivery', scope)
        assert sum(e['type'] == 'mq_received' for e in full['data']['events']) == 2
        assert sum(e['type'] == 'mq_received' for e in samples['delivery'][0]['data']['events']) == 1
        assert len(full['data']['orders']) == 1
        write(self.path / 'partial-redelivery-full-window.json', full)

        await self.coupon(95108, 1, mysql_stock=0)
        await self.clear_stage_window()
        start = now()
        reply = await self.submit(95108, 10006)
        assert reply['body']['success']
        compensated = await self.wait_orders(95108, 0, failed=True)
        assert not compensated['orders'] and compensated['redis_stock'] == 1
        samples = await self.snapshot('compensation-and-drift', start)
        assert any(e['type'] == 'reservation_compensated' for e in samples['events'][0]['data']['events'])
        write(self.path / 'compensation-and-drift-injection.json', {'request': reply, 'final': compensated})

        # Freeze a clearly separated current sample, without rewriting the original window.
        await asyncio.sleep(max(0, (historical_end + 31_000 - now()) / 1000))
        changed_at = now()
        await self.stock(95105, 8)
        samples = await self.snapshot('historical-stock', historical_start, end=historical_end)
        assert samples['inventory'][0]['data']['redisStock'] == '8' and changed_at - historical_end >= 30_000
        write(self.path / 'historical-stock-injection.json', {'event_window_end': historical_end, 'later_write_at': changed_at, 'redis_written': 8})


async def main():
    identifier = datetime.now().strftime('v03-%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:8]
    path = ROOT / 'artifacts/test-runs' / identifier
    (path / 'temp').mkdir(parents=True)
    (path / '.run-owner').write_text(identifier)
    write(path / 'resource_manifest.json', {'run_id': identifier, 'created_at': datetime.now().isoformat(), 'compose_project': 'sltest-' + identifier, 'resources': []})
    frozen = {'created_at': datetime.now().isoformat(), 'tasks': [
        {'id': name, 'voucher_id': voucher, 'expected': root, 'symptom': symptom} for name, (voucher, root, symptom) in CASES.items()
    ]}
    write(path / 'gold.json', frozen)
    write(path / 'gold-sha256.json', {'sha256': hashlib.sha256((path / 'gold.json').read_bytes()).hexdigest()})
    run = Capture(path)
    run.register('directory', str(path / 'temp'), 'Dedicated isolated transient data')
    run.ops_admin = secrets.token_urlsafe(32)
    run.ops_java = secrets.token_urlsafe(32)
    run.env.update(OPS_ADMIN_TOKEN=run.ops_admin, OPS_JAVA_TOKEN=run.ops_java, HMDP_OPS_ADMIN_TOKEN=run.ops_java)
    write(ROOT / 'artifacts/p41-current.json', {'run': str(path)})
    print('P41_CAPTURE ' + str(path), flush=True)
    try:
        run.sample('before', guard=True)
        run.build()
        await run.infrastructure()
        run.monitor_thread = threading.Thread(target=run.monitor, daemon=True)
        run.monitor_thread.start()
        await run.start_agent()
        await run.auth_checks()
        await run.capture_cases()
        run.sample('all-faults-captured', guard=True)
    except BaseException as exc:
        run.failure = f'{type(exc).__name__}: {exc}'
        write(path / 'capture-failure.json', {'error': run.failure, 'traceback': traceback.format_exc()})
        raise
    finally:
        if run.env.get('AGENT_TASK_DB'):
            run.backup_tasks()
        run.cleanup()
    write(path / 'capture-complete.json', {'cases': len(CASES), 'model_calls_during_capture': 0})


if __name__ == '__main__':
    asyncio.run(main())
