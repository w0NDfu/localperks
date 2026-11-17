"""Exercise delayed-close recovery and existing stock compensation in an isolated stack."""
import asyncio
import time

import httpx

from run_perf import patch_source
from run_scale_jmeter import HERE, ScaleRun, write


class RecoveryRun(ScaleRun):
    def prepare_source(self, work):
        path = work / 'src/main/java/com/hmdp/mq/OrderTimeoutMessageService.java'
        anchor = '        for (MqMessage row : pending) {'
        patch_source(path, [(anchor, anchor + '''
            if (row.getVoucherId().equals(98212L) && !java.nio.file.Files.exists(
                    java.nio.file.Paths.get(System.getenv("SHOPLINK_RECOVERY_GATE")))) {
                continue;
            }''')])


async def submit(run, voucher, uid):
    async with httpx.AsyncClient(base_url=run.java_url, timeout=10, trust_env=False) as client:
        response = await client.post(f'/voucher-order/seckill/{voucher}',
                                     headers={'authorization': run.tokens[uid]})
        response.raise_for_status()
        return response.json()['data']['orderId']


async def observe(run, voucher, condition):
    deadline = time.monotonic() + 35
    state = None
    while time.monotonic() < deadline:
        state = await run.snapshot_voucher(voucher)
        if condition(state):
            return state
        await asyncio.sleep(.5)
    write(run.path / f'last-observation-{voucher}.json', state)
    raise AssertionError(f'Timeout state not reached for voucher {voucher}')


async def main():
    run = RecoveryRun()
    run.plan = HERE / 'scale_seckill_warm.jmx'
    gate = run.path / 'temp' / 'allow-timeout-dispatch'
    run.env['SHOPLINK_RECOVERY_GATE'] = str(gate)
    print('TIMEOUT_RECOVERY ' + str(run.path), flush=True)
    try:
        run.sample('before', guard=True)
        run.build()
        await run.infrastructure()
        await run.coupon(98211, 3)
        normal_id = await submit(run, 98211, 10001)
        normal = await observe(run, 98211, lambda state: state['timeout_outbox'] and
                               state['timeout_outbox'][0]['status'] == 1)
        assert str(normal['orders'][0]['id']) == normal_id and normal['orders'][0]['status'] == 1
        assert normal['timeout_outbox'][0]['message_id'] not in ('local-timeout', None)
        write(run.path / 'timeout-acked.json', normal)

        await run.coupon(98212, 3)
        order_id = await submit(run, 98212, 10002)
        committed = await observe(run, 98212, lambda state: state['orders'] and state['timeout_outbox'])
        assert committed['timeout_outbox'][0]['retry_count'] == 0 and committed['orders'][0]['status'] == 1
        write(run.path / 'committed-before-fault.json', committed)
        broker = run.containers['broker']
        run.command(['docker', 'pause', broker])
        try:
            gate.touch()
            pending = await observe(run, 98212, lambda state: state['orders'] and state['timeout_outbox'] and
                                    state['timeout_outbox'][0]['last_error'])
            assert str(pending['orders'][0]['id']) == order_id and pending['orders'][0]['status'] == 1
            assert pending['timeout_outbox'][0]['status'] == 0
            write(run.path / 'timeout-send-failed-but-committed.json', pending)
            run.sql(f"""UPDATE tb_mq_message SET create_time=DATE_SUB(NOW(), INTERVAL 6 MINUTE),
                    next_retry_time=NOW() WHERE order_id={int(order_id)} AND topic='voucher-order-timeout-topic';""")
            closed = await observe(run, 98212, lambda state:
                                   state['timeout_outbox'][0]['message_id'] == 'local-timeout' and
                                   state['orders'][0]['status'] == 4 and
                                   state['mysql_stock'] == state['redis_stock'] == 3 and
                                   not state['redis_qualified_users'])
            write(run.path / 'timeout-due-fallback.json', closed)
        finally:
            run.command(['docker', 'unpause', broker])

        await run.coupon(98213, 1, mysql_stock=0)
        await submit(run, 98213, 10003)
        compensated = await run.wait_orders(98213, 0, failed=True)
        assert compensated['mysql_stock'] == 0 and compensated['redis_stock'] == 1
        assert not (compensated['orders'] or compensated['timeout_outbox'] or compensated['redis_qualified_users'])
        write(run.path / 'reservation-compensated.json', compensated)
        write(run.path / 'timeout-recovery-result.json', {'broker_ack': True, 'dedicated_broker_paused': True,
              'committed_before_timeout_send': True, 'due_fallback_restored_stock': True,
              'existing_mysql_failure_compensation': True})
    except Exception as error:
        run.failure = str(error)
        raise
    finally:
        run.cleanup()


if __name__ == '__main__':
    asyncio.run(main())
