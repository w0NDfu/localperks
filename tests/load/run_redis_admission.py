"""Real Redis/MySQL/RocketMQ regressions for admission without foreground SQL.

Creates only manifest-owned disposable containers. Uses the unmodified production JAR.
No model API calls or performance claims; the MySQL pause verifies the admission boundary.
"""
import asyncio
import hashlib
import json
import shutil
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
from redis.asyncio import Redis

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tests/real_e2e'))
from run import Run, write


class RedisAdmissionRun(Run):
    def __init__(self):
        super().__init__()
        # Accelerate only background discovery so fixtures do not wait one minute each.
        self.java_app_options = ['--hmdp.seckill.preheat-fixed-delay=1000']

    def build(self):
        result = self.command([shutil.which('mvn.cmd'), '-o', '-q', '-DskipTests', 'package'], timeout=150)
        (self.path / 'maven-package.log').write_text(result.stdout + result.stderr, encoding='utf-8')
        self.jar = ROOT / 'target/localperks-backend-0.0.1-SNAPSHOT.jar'
        write(self.path / 'versions.json', {
            'jar_sha256': hashlib.sha256(self.jar.read_bytes()).hexdigest(),
            'normal_volumes_before': self.command(['docker', 'volume', 'ls', '--format', '{{.Name}}']).stdout.splitlines(),
            'production_sources_unmodified': True,
            'test_override': 'background preheat interval 1000ms; default rate limits unchanged',
        })

    async def coupon(self, identifier, stock, future=False, history=None):
        if history is not None:
            # Legacy Redis inventory already exists during an upgrade; never replace it from SQL.
            async with Redis.from_url(self.redis_url) as redis:
                await redis.set(f'seckill:stock:{identifier}', stock)
        begin = 'NOW()+INTERVAL 1 DAY' if future else 'NOW()-INTERVAL 1 DAY'
        statement = (f"START TRANSACTION; INSERT INTO tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) "
                     f"VALUES({identifier},9001,'Admission fixture','test',100,100,1,1); "
                     f"INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) "
                     f"VALUES({identifier},{stock},{begin},NOW()+INTERVAL 7 DAY); ")
        if history is not None:
            statement += (f"INSERT INTO tb_voucher_order(id,user_id,voucher_id,status) "
                          f"VALUES({history},10004,{identifier},4); ")
        self.sql(statement + 'COMMIT;')
        async with Redis.from_url(self.redis_url, decode_responses=True) as redis:
            for _ in range(120):
                if await redis.hget(f'seckill:activity:{identifier}', 'ready') == '1':
                    return
                await asyncio.sleep(.1)
        raise AssertionError(f'Background preheat did not prepare {identifier}')


async def main():
    run = RedisAdmissionRun()
    print('REDIS_ADMISSION ' + str(run.path), flush=True)
    result = {}
    paused = False
    try:
        run.build()
        await run.infrastructure()
        await run.coupon(880001, 3)
        await run.coupon(880002, 3, future=True)
        await run.coupon(880003, 0)
        await run.coupon(880004, 3, history=88000401)
        async with Redis.from_url(run.redis_url, decode_responses=True) as redis, httpx.AsyncClient(
                base_url=run.java_url, timeout=4, trust_env=False) as client:
            async def submit(voucher, user):
                started = time.perf_counter()
                response = await client.post(f'/voucher-order/seckill/{voucher}',
                                             headers={'authorization': run.tokens[user]})
                row = {'http': response.status_code, 'body': response.json(),
                       'elapsed_ms': round((time.perf_counter() - started) * 1000, 2)}
                assert response.status_code == 200, row
                return row

            run.stage('mysql-paused-admission-still-responds')
            run.command(['docker', 'pause', run.containers['mysql']])
            paused = True
            try:
                accepted = await submit(880001, 10001)
                assert accepted['body']['data']['requestState'] == 'ACCEPTED', accepted
                order = accepted['body']['data']['orderId']
                duplicate = await submit(880001, 10001)
                assert duplicate['body']['data']['orderId'] == order, duplicate
                future = await submit(880002, 10002)
                sold = await submit(880003, 10003)
                historical = await submit(880004, 10004)
                missing = await submit(889999, 10005)
                assert not future['body']['success'] and '时段' in future['body']['errorMsg'], future
                assert not sold['body']['success'] and '库存' in sold['body']['errorMsg'], sold
                assert historical['body']['data']['orderId'] == '88000401', historical
                assert not missing['body']['success'] and '就绪' in missing['body']['errorMsg'], missing
                result['mysql_paused'] = {'accepted': accepted, 'duplicate': duplicate, 'future': future,
                                          'sold_out': sold, 'historical': historical, 'missing': missing}
            finally:
                run.command(['docker', 'unpause', run.containers['mysql']])
                paused = False

            for _ in range(120):
                status = await client.get(f'/voucher-order/status/{order}', headers={'authorization': run.tokens[10001]})
                if status.json().get('data', {}).get('statusCode') == 12:
                    break
                await asyncio.sleep(.25)
            else:
                raise AssertionError('Order did not become payable after MySQL resumed')
            result['async_order_after_mysql_resume'] = status.json()
            assert run.sql('SELECT stock FROM tb_seckill_voucher WHERE voucher_id=880001') == [['2']]
            assert await redis.get('seckill:stock:880001') == '2'
            cancelled = await client.post(f'/voucher-order/cancel/{order}', headers={'authorization': run.tokens[10001]})
            assert cancelled.json()['data']['statusCode'] == 14, cancelled.text
            await client.post(f'/voucher-order/cancel/{order}', headers={'authorization': run.tokens[10001]})
            assert await redis.get('seckill:stock:880001') == '3'
            assert run.sql('SELECT stock FROM tb_seckill_voucher WHERE voucher_id=880001') == [['3']]
            repeated = await submit(880001, 10001)
            assert repeated['body']['data']['orderId'] == order, repeated
            assert run.sql('SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=880001') == [['1']]
            result['cancelled_purchase_is_still_unique'] = {'cancel': cancelled.json(), 'repeat': repeated,
                                                          'redis_stock': '3', 'sql_stock': 3}
            # Simulate a committed cancellation whose after-commit Redis release was lost.
            accepted2 = await submit(880001, 10006)
            order2 = accepted2['body']['data']['orderId']
            for _ in range(120):
                if run.sql(f'SELECT id FROM tb_voucher_order WHERE id={int(order2)}'):
                    break
                await asyncio.sleep(.25)
            run.sql(f'START TRANSACTION; UPDATE tb_voucher_order SET status=4 WHERE id={int(order2)}; '
                    'UPDATE tb_seckill_voucher SET stock=stock+1 WHERE voucher_id=880001; COMMIT;')
            for _ in range(80):
                if await redis.get('seckill:stock:880001') == '3':
                    break
                await asyncio.sleep(.1)
            else:
                raise AssertionError('Committed cancellation was not reconciled')
            result['lost_cancel_release_reconciled'] = True
            await run.coupon(880005, 3)
            burst = await asyncio.gather(*(submit(880005, user) for user in range(10011, 10021)))
            assert sum(row['body']['success'] for row in burst) == 3, burst
            assert sum('库存不足' == row['body'].get('errorMsg') for row in burst) == 7, burst
            for _ in range(120):
                if run.sql('SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=880005') == [['3']]:
                    break
                await asyncio.sleep(.25)
            assert run.sql('SELECT stock FROM tb_seckill_voucher WHERE voucher_id=880005') == [['0']]
            assert await redis.get('seckill:stock:880005') == '0'
            result['concurrent_three_coupons'] = {'requests': 10, 'accepted': 3, 'sold_out': 7,
                                                 'orders': 3, 'redis_stock': 0, 'mysql_stock': 0}
            # Exercise real transaction commit -> warmup through the activity publishing API.
            now = run.sql("SELECT DATE_FORMAT(NOW()-INTERVAL 1 HOUR,'%Y-%m-%dT%H:%i:%s'),"
                          "DATE_FORMAT(NOW()+INTERVAL 1 DAY,'%Y-%m-%dT%H:%i:%s')")[0]
            published = await client.post('/voucher/seckill', headers={'authorization': run.tokens[10008]}, json={
                'shopId': 9001, 'title': 'After-commit warmup fixture', 'rules': 'test', 'payValue': 100,
                'actualValue': 100, 'stock': 2, 'beginTime': now[0], 'endTime': now[1]})
            assert published.json()['success'], published.text
            published_id = published.json()['data']
            assert await redis.hget(f'seckill:activity:{published_id}', 'ready') == '1'
            published_order = await submit(published_id, 10008)
            assert published_order['body']['data']['requestState'] == 'ACCEPTED', published_order
            result['published_activity_ready_after_commit'] = True
            await redis.delete('seckill:stock:880001')
            lost = await submit(880001, 10007)
            assert not lost['body']['success'] and '就绪' in lost['body']['errorMsg'], lost
            await asyncio.sleep(1.5)
            assert await redis.get('seckill:stock:880001') is None
            result['missing_live_stock_not_refilled'] = lost

        # Stop the application before isolated Java integration tests mutate their own fixtures.
        for process in run.processes:
            run.command(['taskkill', '/PID', str(process.pid), '/T', '/F'], check=False)
            process.wait(timeout=15)
        run.stage('real-redis-mysql-java-regressions')
        test = run.command([shutil.which('mvn.cmd'), '-o', '-q',
                            '-Dtest=SeckillActivityCacheIntegrationTest,VoucherSeckillPublicationTest,VoucherOrderAdmissionTest,VoucherOrderOwnershipTest,SeckillOrderMessageServiceTest,SeckillBrokerBatchSenderTest,OrderTimeoutMessageServiceTest,OrderTimeoutClaimRepositoryTest',
                            f'-Dvalidation.mysql.port={run.mysql_port}', f'-Dvalidation.redis.port={run.redis_port}', 'test'],
                           env=dict(run.env, VALIDATION_MYSQL_PASSWORD=run.mysql_password), timeout=150, check=False)
        (run.path / 'maven-regression.log').write_text(test.stdout + test.stderr, encoding='utf-8')
        if test.returncode:
            raise AssertionError('Java regressions failed; see maven-regression.log')
        suites = []
        selected = ('SeckillActivityCacheIntegrationTest', 'VoucherSeckillPublicationTest', 'VoucherOrderAdmissionTest',
                    'VoucherOrderOwnershipTest', 'SeckillOrderMessageServiceTest', 'SeckillBrokerBatchSenderTest',
                    'OrderTimeoutMessageServiceTest', 'OrderTimeoutClaimRepositoryTest')
        reports = run.path / 'surefire'
        reports.mkdir()
        for report in (ROOT / 'target/surefire-reports').glob('TEST-*.xml'):
            if report.stem.split('.')[-1] in selected:
                shutil.copy2(report, reports / report.name)
                suite = ET.parse(report).getroot()
                suites.append({key: suite.attrib[key] for key in ('name', 'tests', 'failures', 'errors', 'skipped')})
        result['java_regressions'] = {'suites': suites, 'tests': sum(int(suite['tests']) for suite in suites)}
        result['pass'] = True
    except Exception as error:
        run.failure = type(error).__name__ + ': ' + str(error)
        result.update({'pass': False, 'failure': run.failure})
        raise
    finally:
        if paused:
            run.command(['docker', 'unpause', run.containers['mysql']], check=False)
        write(run.path / 'redis-admission-results.json', result)
        run.cleanup()


if __name__ == '__main__':
    asyncio.run(main())
