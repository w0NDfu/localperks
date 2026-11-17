"""Broker-confirmed admission: real isolated middleware faults, no admission SQL Outbox."""
import asyncio
import json
import subprocess
import sys
import threading
import time

import httpx
from redis.asyncio import Redis
from run_http_perf import HttpPerfRun, ROOT, patch_source, seed_users, write

sys.path.insert(0, str(ROOT / 'tests/final_agent'))
from scale_capture import FaultCapture


class BrokerFaultRun(HttpPerfRun):
    def prepare_source(self, work):
        super().prepare_source(work)
        java = work / 'src/main/java/com/hmdp'
        patch_source(java / 'mq/SeckillOrderMessageService.java', [
            ('        return result;', '''        if (message.getVoucherId() == 98261L && System.getProperties().remove("p8.loseBrokerAck") != null) {
            log.warn("P8_ACTUAL_BROKER_ACK_LOST orderId={} messageId={}", message.getOrderId(), result.getMsgId());
            throw new IllegalStateException("P8 injected lost ACK after actual SEND_OK");
        }
        return result;'''),
        ])

    async def wait_orders(self, voucher, expected, failed=False):
        for _ in range(100):
            state = await self.snapshot_voucher(voucher)
            async with Redis.from_url(self.redis_url, decode_responses=True) as redis:
                ids = await redis.zrange('seckill:admission:pending', 0, -1)
                rows = [await redis.hgetall('seckill:request:' + item) for item in ids]
                pending = [r for r in rows if r.get('voucherId') == str(voucher)]
            if len(state['orders']) == expected and not pending and (
                    not failed or state['outbox'] and all(r['status'] == 2 for r in state['outbox'])):
                return state
            await asyncio.sleep(.5)
        raise AssertionError(f'Order/receipt convergence failed: {voucher}: {state}')


async def restart(run):
    previous = run.processes[-1]
    run.command(['taskkill', '/PID', str(previous.pid), '/T', '/F'])
    previous.wait(timeout=15)
    stream = (run.path / 'java.log').open('a', encoding='utf-8')
    run.files.append(stream)
    # One-shot gap injections must not re-arm after restarting the real process.
    arguments = [arg for arg in previous.args if not str(arg).startswith(('-Dp8.beforeSend=', '-Dp8.loseBrokerAck='))]
    process = subprocess.Popen(arguments, cwd=ROOT, env=run.env, stdout=stream, stderr=subprocess.STDOUT)
    run.processes.append(process)
    async with httpx.AsyncClient(timeout=3, trust_env=False) as client:
        for _ in range(65):
            if process.poll() is not None:
                raise RuntimeError('Restarted Java exited')
            try:
                response = await client.get(run.java_url + '/shop-type/list')
                if response.status_code == 200 and response.json().get('success'):
                    return {'old_pid': previous.pid, 'new_pid': process.pid, 'old_exit': previous.returncode}
            except httpx.HTTPError:
                pass
            await asyncio.sleep(1)
    raise RuntimeError('Restart readiness timeout')


async def main():
    run = BrokerFaultRun(100, 'p8-broker-confirmed-faults')
    run.storage_profile = 'disk'
    run.java_vm_options.append('-Dp8.loseBrokerAck=true')
    run.java_app_options.append('--hmdp.seckill.max-pending=4')
    print('HTTP_FAULTS ' + str(run.path), flush=True)
    write(run.path / 'frozen-faults.json', {
        'admission': 'No new SQL admission rows; only SEND_OK permits ACCEPTED',
        'ack_loss': 'Inject once AFTER a real Broker SEND_OK; keep original ID and UNKNOWN',
        'broker_pause': 'Actual Docker Broker pause: must return UNKNOWN, restart Java and recover',
        'before_send_exit': 'Create reservation with the real Redis Lua, then terminate and restart Java before any sender claims it',
        'others': ['duplicate requests', 'real message replay', 'backpressure', 'terminal compensation',
                   'old failure cannot roll back newer reservation', 'lease ownership', 'wallet visibility',
                   'legacy SQL row drain', 'activity bounds', 'script cache flush']})
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
        async with Redis.from_url(run.redis_url, decode_responses=True) as redis, httpx.AsyncClient(
                base_url=run.java_url, timeout=10, trust_env=False) as client:
            async def status(order, uid):
                response = await client.get('/voucher-order/status/' + order, headers={'authorization': run.tokens[uid]})
                response.raise_for_status()
                return response.json()

            async def wallet(uid):
                response = await client.get('/voucher-order/mine', headers={'authorization': run.tokens[uid]})
                response.raise_for_status()
                return response.json()

            await run.coupon(98261, 10)
            run.stage('broker-ack-loss-backpressure-and-wallet')
            fault.consume(False, 'p8-ack')
            try:
                unknown = await fault.submit(98261, 10001)
                assert unknown['body']['data']['requestState'] == 'UNKNOWN', unknown
                oid = unknown['body']['data']['orderId']
                repeated = await fault.submit(98261, 10001)
                assert repeated['body']['data']['orderId'] == oid
                assert (await status(oid, 10001))['data']['requestState'] in ('UNKNOWN', 'ACCEPTED')
                assert not (await status(oid, 10002))['success']
                history = await wallet(10001)
                assert any(r['orderId'] == oid for r in history['data']['records']), history
                for user in (10002, 10003, 10004):
                    response = await fault.submit(98261, user)
                    assert response['body']['data']['requestState'] == 'ACCEPTED', response
                rejected = await fault.submit(98261, 10005)
                assert rejected['http'] == 429
                before = await run.snapshot_voucher(98261)
                assert not before['orders'] and not before['outbox'] and before['redis_stock'] == 6
                results['lost_ack_pending_and_backpressure'] = {'unknown': unknown, 'repeat': repeated,
                    'wallet': history, 'rejection': rejected, 'snapshot': before}
            finally:
                fault.consume(True, 'p8-ack')
            recovered = await run.wait_orders(98261, 4)
            assert recovered['mysql_stock'] == recovered['redis_stock'] == 6 and not recovered['outbox']
            sent = await asyncio.to_thread(fault.replay, oid)
            assert 'VALIDATION_SEND SEND_OK' in sent
            await asyncio.sleep(1)
            replayed = await run.snapshot_voucher(98261)
            assert len(replayed['orders']) == 4 and replayed['mysql_stock'] == replayed['redis_stock'] == 6
            results['duplicate_delivery'] = replayed

            await run.clear_stage_window()
            await run.coupon(98262, 5)
            run.stage('broker-pause-and-process-restart')
            run.command(['docker', 'pause', run.containers['broker']])
            paused = True
            try:
                started = time.monotonic()
                response = await fault.submit(98262, 10021)
                assert response['body']['data']['requestState'] == 'UNKNOWN', response
                state = await run.snapshot_voucher(98262)
                assert not state['orders'] and not state['outbox'] and state['redis_stock'] == 4
                results['broker_pause'] = {'response': response, 'elapsed_ms': round((time.monotonic()-started)*1000), 'snapshot': state}
            finally:
                run.command(['docker', 'unpause', run.containers['broker']])
                paused = False
            results['restart_after_uncertain_send'] = await restart(run)
            state = await run.wait_orders(98262, 1)
            assert state['mysql_stock'] == state['redis_stock'] == 4 and not state['outbox']
            results['restart_after_uncertain_send']['final'] = state

            await run.clear_stage_window()
            await run.coupon(98267, 2)
            run.stage('process-exit-before-send')
            # Construct the exact pre-send state with the real Lua, then terminate the process.
            oid = '640000000000098267'
            reserve = (ROOT / 'src/main/resources/seckill.lua').read_text(encoding='utf-8')
            assert await redis.eval(reserve, 0, '98267', '10043', oid, str(int(time.time()*1000)), '4') == 0
            results['restart_before_send'] = await restart(run)
            state = await run.wait_orders(98267, 1)
            assert str(state['orders'][0]['id']) == oid and not state['outbox']
            results['restart_before_send']['final'] = state

            await run.clear_stage_window()
            await run.coupon(98263, 1, mysql_stock=0)
            run.stage('terminal-compensation-and-new-owner')
            failure = await fault.submit(98263, 10031)
            assert failure['body']['data']['requestState'] == 'ACCEPTED'
            failed_id = failure['body']['data']['orderId']
            state = await run.wait_orders(98263, 0, failed=True)
            assert state['redis_stock'] == 1 and state['mysql_stock'] == 0
            assert (await status(failed_id, 10031))['data']['requestState'] == 'FAILED'
            # Reserve a new attempt; an old terminal delivery must not release the new attempt's stock.
            new_id = '640000000000098263'
            assert await redis.eval(reserve, 0, '98263', '10031', new_id, str(int(time.time()*1000)), '4') == 0
            token = 'test-owned-send-lease'
            assert await redis.set('seckill:request:' + new_id + ':send-lock', token, nx=True, px=20000)
            sent = await asyncio.to_thread(fault.replay, failed_id)
            assert 'VALIDATION_SEND SEND_OK' in sent
            await asyncio.sleep(1)
            assert int(await redis.get('seckill:stock:98263')) == 0
            assert await redis.sismember('seckill:order:98263', '10031')
            rollback = (ROOT / 'src/main/resources/seckill_rollback.lua').read_text(encoding='utf-8')
            await redis.eval(rollback, 0, '98263', '10031', failed_id)
            assert int(await redis.get('seckill:stock:98263')) == 0
            # Allow the new attempt to receive its own terminal failure and compensation.
            await redis.delete('seckill:request:' + new_id + ':send-lock')
            final = await run.wait_orders(98263, 0, failed=True)
            assert final['redis_stock'] == 1
            results['terminal_compensation_and_new_owner'] = {'original': failure, 'final': final}

            await run.coupon(98264, 5)
            run.stage('eligibility-script-flush-and-legacy-drain')
            legacy_id = '640000000000000064'
            run.sql(f"INSERT INTO tb_voucher_order(id,user_id,voucher_id,status) VALUES({legacy_id},10041,98264,1)")
            legacy = await fault.submit(98264, 10041)
            assert legacy['body']['data']['orderId'] == legacy_id
            await run.coupon(98265, 5, future=True)
            future = await fault.submit(98265, 10042)
            assert not future['body']['success'] and '时段' in future['body']['errorMsg']
            results['eligibility'] = {'legacy': legacy, 'future': future}
            await run.coupon(98269, 5)
            run.sql("UPDATE tb_seckill_voucher SET begin_time=DATE_SUB(NOW(),INTERVAL 2 DAY),end_time=DATE_SUB(NOW(),INTERVAL 1 DAY) WHERE voucher_id=98269")
            await run.coupon(98270, 2)
            run.sql(f"UPDATE tb_voucher_order SET status=2 WHERE id={legacy_id}")
            mixed = await asyncio.gather(fault.submit(98264, 10041), fault.submit(98265, 10042),
                                        fault.submit(98269, 10046), fault.submit(98270, 10047), fault.submit(98999, 10048))
            assert mixed[0]['body']['data']['orderId'] == legacy_id and mixed[0]['body']['data']['statusCode'] == 13
            assert not mixed[1]['body']['success'] and not mixed[2]['body']['success'] and not mixed[4]['body']['success']
            assert mixed[3]['body']['data']['requestState'] == 'ACCEPTED'
            valid_final = await run.wait_orders(98270, 1)
            assert valid_final['mysql_stock'] == valid_final['redis_stock'] == 1
            assert int(await redis.get('seckill:stock:98269')) == 5
            results['mixed_fresh_eligibility'] = {'responses': mixed, 'valid_final': valid_final, 'expired_stock': 5}
            derived = ' UNION ALL '.join(f'SELECT {index} AS slot,{10041+index} AS uid,{98264+index%2} AS vid' for index in range(32))
            explain = run.sql('EXPLAIN SELECT r.slot,o.id AS order_id,o.user_id,o.voucher_id,o.status,o.pay_time,o.create_time,s.begin_time,s.end_time FROM ('
                              + derived + ') r LEFT JOIN tb_seckill_voucher s ON s.voucher_id=r.vid LEFT JOIN tb_voucher_order o ON o.user_id=r.uid AND o.voucher_id=r.vid')
            write(run.path / 'eligibility-batch-explain.json', explain)

            await redis.script_flush()
            await run.clear_stage_window()
            await run.coupon(98266, 2)
            flush = await fault.submit(98266, 10044)
            assert flush['body']['data']['requestState'] == 'ACCEPTED'
            final = await run.wait_orders(98266, 1)
            assert final['mysql_stock'] == final['redis_stock'] == 1
            results['script_flush'] = final
            # Actual pre-upgrade SQL row, no Redis receipt. Verify migration draining remains operational.
            await run.coupon(98268, 2)
            legacy_order = '640000000000098268'
            body = json.dumps({'orderId': int(legacy_order), 'userId': 10045, 'voucherId': 98268})
            await redis.set('seckill:stock:98268', 1)
            await redis.sadd('seckill:order:98268', '10045')
            run.sql(f"INSERT INTO tb_mq_message(order_id,user_id,voucher_id,topic,message_body) VALUES({legacy_order},10045,98268,'seckill-order-topic','{body}')")
            final = await run.wait_orders(98268, 1)
            assert final['mysql_stock'] == final['redis_stock'] == 1
            results['legacy_outbox_drain'] = final
            results['pass'] = True
            write(run.path / 'fault-results.json', results)
    except BaseException as error:
        write(run.path / 'fault-results.json', {**results, 'pass': False, 'error': type(error).__name__, 'detail': str(error)})
        raise
    finally:
        if paused:
            run.command(['docker', 'unpause', run.containers['broker']], check=False)
        run.cleanup()


if __name__ == '__main__':
    asyncio.run(main())
