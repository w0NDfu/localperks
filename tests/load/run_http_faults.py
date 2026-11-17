"""P6 real Broker/SQL safety regressions in a disposable stack."""
import asyncio
import json
import sys
import time
import threading
import subprocess
from pathlib import Path

import httpx
from redis.asyncio import Redis

from run_http_perf import HttpPerfRun, patch_source, ROOT, HERE, seed_users, write
sys.path.insert(0, str(ROOT / 'tests/final_agent'))
from scale_capture import FaultCapture


class FaultRun(HttpPerfRun):
    def prepare_source(self, work):
        super().prepare_source(work)
        java = work / 'src/main/java/com/hmdp'
        patch_source(java / 'mq/SeckillAdmissionRepository.java', [
            ('        outbox.persist(row);',
             '''        outbox.persist(row);
        if (row.getVoucherId() == 98261L && System.getProperties().remove("p6.loseSqlAck") != null) {
            throw new IllegalStateException("P6 injected: actual SQL commit succeeded, reply lost");
        }'''),
        ])
        patch_source(java / 'mq/SeckillOrderMessageService.java', [
            ('            public void onSuccess(SendResult result) {',
             '''            public void onSuccess(SendResult result) {
                if (message.getVoucherId() == 98261L && result.getSendStatus() == SendStatus.SEND_OK
                        && System.getProperties().remove("p6.loseBrokerAck") != null) {
                    log.warn("P6_ACTUAL_BROKER_ACK_LOST orderId={} messageId={}", message.getOrderId(), result.getMsgId());
                    onException(new IllegalStateException("P6 injected: actual Broker appended, ACK lost"));
                    return;
                }'''),
        ])


async def main():
    run = FaultRun(100, 'p6-failure-regression')
    run.env['HMDP_OPS_ADMIN_TOKEN'] = 'isolated-p6-ops-' + run.id
    run.java_vm_options.extend(['-Dp6.loseSqlAck=true', '-Dp6.loseBrokerAck=true'])
    run.java_app_options.append('--hmdp.seckill.max-pending=4')
    write(run.path / 'frozen-faults.json', {
        'unknown_commit_and_ack': '98261, consumer paused; actual SQL commit and actual Broker append, inject lost acknowledgements once',
        'backpressure': 'maximum four outstanding, fifth unique user rejected without a reservation',
        'broker_pause': '98262, actual Docker pause/unpause after durable HTTP admission; terminate and restart Java before recovery',
        'stock_failure': '98263, Redis stock 1, SQL stock 0, consumer terminal compensation',
        'duplicate': 'replay the original real Broker body and verify unchanged order and stock',
        'script_cache_flush': 'clear only the dedicated test Redis script cache and verify EVALSHA fallback with preserved data',
        'injected_reply_loss_is_distinct_from_network_fault': True})
    print('HTTP_FAULTS ' + str(run.path), flush=True)
    results = {}
    paused = False
    try:
        run.sample('before', guard=True)
        run.build()
        await run.infrastructure()
        run.monitor_thread = threading.Thread(target=run.monitor, daemon=True)
        run.monitor_thread.start()
        await seed_users(run, 100)
        fault = FaultCapture(run, run.env['HMDP_OPS_ADMIN_TOKEN'])
        async with Redis.from_url(run.redis_url, decode_responses=True) as redis:
            await run.coupon(98261, 10)
            fault.consume(False, 'p6-unknown')
            try:
                started = time.perf_counter()
                unknown = await fault.submit(98261, 10001)
                assert unknown['body']['data']['requestState'] == 'UNKNOWN'
                unknown_id = unknown['body']['data']['orderId']
                assert len(run.sql(f"SELECT id FROM tb_mq_message WHERE order_id={int(unknown_id)}")) == 1
                assert int(await redis.get('seckill:stock:98261')) == 9
                others = [await fault.submit(98261, user) for user in (10002, 10003, 10004)]
                assert all(item['body']['data']['requestState'] == 'ACCEPTED' for item in others)
                rejected = await fault.submit(98261, 10005)
                membership = await redis.sismember('seckill:order:98261', '10005')
                write(run.path / 'backpressure-check.json', {'response': rejected, 'membership': membership,
                                                             'pending': await redis.zcard('seckill:admission:pending')})
                assert rejected['http'] == 429 and not membership
                repeated = await fault.submit(98261, 10001)
                assert repeated['http'] == 200 and repeated['body']['data']['orderId'] == unknown_id
                for _ in range(16):
                    if 'P6_ACTUAL_BROKER_ACK_LOST' in (run.path / 'java.log').read_text(encoding='utf-8'):
                        break
                    await asyncio.sleep(.5)
                pre = await run.snapshot_voucher(98261)
                assert len(pre['orders']) == 0 and len(pre['outbox']) == 4 and pre['redis_stock'] == 6
                assert await redis.zcard('seckill:admission:pending') == 4
                assert 'P6_ACTUAL_BROKER_ACK_LOST' in (run.path / 'java.log').read_text(encoding='utf-8')
                results['before_consumer_resume'] = {'unknown': unknown, 'other_requests': others,
                    'backpressure': rejected, 'same_user_retry_at_capacity': repeated,
                    'snapshot': pre, 'observation_ms': (time.perf_counter() - started) * 1000}
                write(run.path / 'http-before-orders.json', results['before_consumer_resume'])
            finally:
                fault.consume(True, 'p6-unknown')
            final = await run.wait_orders(98261, 4)
            assert final['redis_stock'] == final['mysql_stock'] == 6
            await asyncio.sleep(1)
            assert await redis.zcard('seckill:admission:pending') == 0
            sent = await asyncio.to_thread(fault.replay, unknown_id)
            assert 'VALIDATION_SEND SEND_OK' in sent
            await asyncio.sleep(2)
            duplicate = await run.snapshot_voucher(98261)
            assert len(duplicate['orders']) == 4 and duplicate['mysql_stock'] == duplicate['redis_stock'] == 6
            results['unknown_recovery_and_duplicate'] = duplicate

            await run.clear_stage_window()
            await run.coupon(98262, 5)
            run.command(['docker', 'pause', run.containers['broker']])
            paused = True
            try:
                start = time.perf_counter()
                accepted = await fault.submit(98262, 10021)
                elapsed = (time.perf_counter() - start) * 1000
                assert accepted['body']['data']['requestState'] == 'ACCEPTED'
                for _ in range(20):
                    await asyncio.sleep(.5)
                    snapshot = await run.snapshot_voucher(98262)
                    if snapshot['outbox'] and snapshot['outbox'][0]['last_error']:
                        break
                assert len(snapshot['orders']) == 0 and snapshot['redis_stock'] == 4
                assert snapshot['outbox'][0]['status'] == 0 and snapshot['outbox'][0]['last_error']
                results['broker_paused'] = {'request': accepted, 'http_elapsed_ms': elapsed, 'snapshot': snapshot}
                write(run.path / 'broker-pause-evidence.json', results['broker_paused'])
                # Prove recovery from SQL, not from the original process's executor queue.
                previous = run.processes[-1]
                run.command(['jcmd', str(previous.pid), 'JFR.dump', 'name=load',
                             'filename=' + (run.path / 'java-before-restart.jfr').as_posix()], check=False)
                run.command(['taskkill', '/PID', str(previous.pid), '/T', '/F'])
                previous.wait(timeout=15)
                results['java_restart'] = {'old_pid': previous.pid, 'old_process_exit': previous.returncode,
                    'accepted_order_id': accepted['body']['data']['orderId'],
                    'sql_outbox_before_restart': snapshot['outbox'],
                    'broker_unavailable_at_original_acceptance': True}
            finally:
                run.command(['docker', 'unpause', run.containers['broker']])
                paused = False
            stream = (run.path / 'java.log').open('a', encoding='utf-8')
            run.files.append(stream)
            restarted = subprocess.Popen(previous.args, cwd=ROOT, env=run.env, stdout=stream, stderr=subprocess.STDOUT)
            run.processes.append(restarted)
            async with httpx.AsyncClient(timeout=3, trust_env=False) as client:
                for _ in range(65):
                    if restarted.poll() is not None:
                        raise RuntimeError('Java recovery process exited')
                    try:
                        ready = await client.get(run.java_url + '/shop-type/list')
                        if ready.status_code == 200 and ready.json().get('success'):
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(2)
                else:
                    raise RuntimeError('Java recovery readiness timeout')
            results['java_restart']['new_pid'] = restarted.pid
            recovered = await run.wait_orders(98262, 1)
            assert recovered['redis_stock'] == recovered['mysql_stock'] == 4
            results['broker_recovered'] = recovered
            results['java_restart']['recovered_orders'] = recovered['orders']
            write(run.path / 'java-restart-evidence.json', results['java_restart'])

            await run.clear_stage_window()
            await run.coupon(98263, 1, mysql_stock=0)
            failure = await fault.submit(98263, 10031)
            assert failure['body']['data']['requestState'] == 'ACCEPTED'
            compensated = await run.wait_orders(98263, 0, failed=True)
            await asyncio.sleep(1)
            assert compensated['redis_stock'] == 1 and compensated['mysql_stock'] == 0
            # Independent repeated rollback must not inflate stock.
            script = (ROOT / 'src/main/resources/seckill_rollback.lua').read_text(encoding='utf-8')
            await redis.eval(script, 0, '98263', '10031')
            await redis.eval(script, 0, '98263', '10031')
            assert int(await redis.get('seckill:stock:98263')) == 1
            assert await redis.zcard('seckill:admission:pending') == 0
            order = failure['body']['data']['orderId']
            async with httpx.AsyncClient(base_url=run.java_url, timeout=10, trust_env=False) as client:
                status = (await client.get('/voucher-order/status/' + order,
                    headers={'authorization': run.tokens[10031]})).json()
            assert status['data']['requestState'] == 'FAILED'
            results['compensation'] = {'initial': failure, 'final': compensated, 'status': status,
                                        'repeated_rollback_stock': 1}
            await run.coupon(98264, 5)
            legacy_id = '640000000000000064'
            run.sql(f"INSERT INTO tb_voucher_order(id,user_id,voucher_id,status) VALUES({legacy_id},10041,98264,1)")
            legacy = await fault.submit(98264, 10041)
            assert legacy['body']['data']['orderId'] == legacy_id
            assert legacy['body']['data']['requestState'] == 'SUCCESS'
            assert int(await redis.get('seckill:stock:98264')) == 5
            await run.coupon(98265, 5, future=True)
            future = await fault.submit(98265, 10042)
            assert future['body']['success'] is False and '时段' in future['body']['errorMsg']
            assert int(await redis.get('seckill:stock:98265')) == 5
            results['legacy_and_activity_checks'] = {'legacy': legacy, 'future_activity': future,
                                                     'legacy_stock': 5, 'future_stock': 5}
            await redis.script_flush()
            await run.clear_stage_window()
            await run.coupon(98266, 2)
            after_flush = await fault.submit(98266, 10043)
            assert after_flush['body']['data']['requestState'] == 'ACCEPTED'
            flush_final = await run.wait_orders(98266, 1)
            assert flush_final['redis_stock'] == flush_final['mysql_stock'] == 1
            results['script_cache_flush'] = {'request': after_flush, 'final': flush_final}
            write(run.path / 'eligibility-explain.json', run.sql(
                "EXPLAIN SELECT o.id,s.begin_time,s.end_time FROM (SELECT 10041 AS uid,98264 AS vid) r "
                "LEFT JOIN tb_seckill_voucher s ON s.voucher_id=r.vid "
                "LEFT JOIN tb_voucher_order o ON o.user_id=r.uid AND o.voucher_id=r.vid"))
            async with httpx.AsyncClient(base_url=run.java_url, timeout=5, trust_env=False) as client:
                for _ in range(12):
                    now = int(time.time() * 1000)
                    response = await client.get('/internal/ops/mq',
                        headers={'X-Ops-Token': run.env['HMDP_OPS_ADMIN_TOKEN']},
                        params={'start': now - 60000, 'end': now, 'voucherId': 98263, 'ip': '127.0.0.1'})
                    response.raise_for_status()
                    mq = response.json()
                    if mq['data']['lag'] == 0 and mq['data']['brokerOffset'] >= 6:
                        break
                    await asyncio.sleep(1)
            assert mq['data']['lag'] == 0 and mq['data']['brokerOffset'] == mq['data']['consumerOffset']
            results['final_mq'] = mq
            results['pass'] = True
            write(run.path / 'fault-results.json', results)
    except BaseException as error:
        write(run.path / 'fault-results.json', {**results, 'pass': False, 'error': type(error).__name__,
                                               'detail': str(error)})
        raise
    finally:
        if paused:
            run.command(['docker', 'unpause', run.containers['broker']], check=False)
        run.cleanup()


if __name__ == '__main__':
    if 'private SendResult sendConfirmed' in (ROOT / 'src/main/java/com/hmdp/mq/SeckillOrderMessageService.java').read_text(encoding='utf-8'):
        from run_broker_faults import main as broker_main
        asyncio.run(broker_main())
    else:
        asyncio.run(main())
