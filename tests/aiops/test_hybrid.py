import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'agent-service'))
from ops.hybrid import UNKNOWN, inspect, investigate, rank_reads


def observation(tool, identifier, data, at=1500):
    return {'id': identifier, 'tool': tool, 'raw': {'data': data, 'observedAt': at, 'windowEnd': 1000}}


def inventory(identifier, db, cache, pending=0, at=1500):
    return observation('inventory', identifier, {'mysql': [{'stock': db}], 'redisStock': cache, 'pending': [{'total': pending}]}, at)


def test_fastpath_can_complete_without_a_model():
    values = {
        'events': {'events': [], 'coverageComplete': True},
        'inventory': {'mysql': [{'stock': 12}], 'redisStock': '10', 'pending': [{'total': 2}]},
        'mq': {'lag': 2, 'brokerOffset': 3, 'consumerOffset': 1, 'consumeEnabled': False, 'brokers': 1},
        'delivery': {'events': [], 'orders': [], 'coverageComplete': True},
    }
    readings = []
    events = []

    async def collect(tools, _):
        for tool in tools:
            readings.append(observation(tool, f'e{len(readings) + 1}', values[tool]))

    async def emit(event):
        events.append(event)

    result, _ = asyncio.run(investigate(SimpleNamespace(), None, '核对尚未落库的订单', readings, collect, emit, emit))
    assert result['root_code'] == 'MQ_CONSUME_DISABLED'
    assert result['route']['path'] == 'rules_fastpath'
    assert len(readings) == 4 and result['actions_executed'] is False
    assert result['trigger_operation']['status'] == 'unconfirmed'


def test_two_proofs_cannot_be_collapsed_into_one_primary():
    readings = [inventory('e1', 12, 8), inventory('e2', 12, 8, at=2000),
                observation('traffic', 'e3', {'events': [{'type': 'http', 'status': 429, 'limitScope': 'ip'}]})]
    checked = inspect(readings, '核对这张券')
    assert {item['code'] for item in checked['verified_categories']} == {'IP_RATE_LIMIT', 'REDIS_STOCK_DRIFT'}
    assert 'multiple_roots' in checked['route_reasons']
    assert set(checked['supported'][UNKNOWN]['evidence_ids']) == {'e1', 'e2', 'e3'}


def test_old_window_cannot_use_later_stock_to_confirm_drift():
    checked = inspect([inventory('e1', 12, 8, at=40_000), inventory('e2', 12, 8, at=41_000)], '核对窗口')
    assert 'historical_gap' in checked['route_reasons']
    assert set(checked['supported']) == {UNKNOWN}


def test_completed_inventory_proof_with_missing_mq_can_check_compensation_once():
    readings = [inventory('e1', 10, 7), {'id': 'e2', 'tool': 'mq', 'error': 'unavailable'}, inventory('e3', 10, 7, at=2000)]
    checked = inspect(readings, '核对补偿')
    assert 'missing_evidence' in checked['route_reasons']
    assert [item['tool'] for item in rank_reads(readings, checked)] == ['compensation']
    readings.append(observation('compensation', 'e4', {'outbox': []}))
    assert rank_reads(readings, inspect(readings, '核对补偿')) == []
