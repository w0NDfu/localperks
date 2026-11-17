import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'agent-service'))
from core.config import Settings
from ops.case_history import CaseHistory, consolidate
from ops.case_index import case_chunks, load_cases, redact
from ops.hybrid import UNKNOWN, inspect, investigate


def observation(tool, identifier, data, at=1500):
    return {'id': identifier, 'tool': tool, 'raw': {'data': data, 'observedAt': at, 'windowEnd': 1000}}


def test_curated_sources_and_consolidation():
    cases = load_cases(ROOT)
    perf = next(case for case in cases if case.id == 'perf-hot-row-outbox')
    packed = consolidate(case_chunks(perf))
    assert len(packed) == 1
    assert packed[0]['matched_sections'] == ['symptoms', 'diagnosis', 'recovery']
    assert '5.289' in packed[0]['metadata']['validation'] and '4.849' in packed[0]['metadata']['validation']
    assert len(packed[0]['metadata']['sources']) > 5
    assert redact('授权 Bearer secret-123 IP 198.19.0.4 电话 19900001111') == '授权 [REDACTED] IP [REDACTED] 电话 [REDACTED]'


def test_history_uses_normalized_query_and_marks_conflicting_live_signal():
    class History(CaseHistory):
        async def search(self, query, **_):
            assert 'RocketMQ' in query and '积压' in query
            assert all(secret not in query for secret in ('secret-123', '198.19.0.4', '710011'))
            case = next(case for case in load_cases(ROOT) if case.id == 'mq-paused-consumption')
            return consolidate(case_chunks(case)), 'rrf', None

    result = asyncio.run(History(Settings(api_key='')).retrieve('RocketMQ 积压 198.19.0.4 710011 secret-123',
        [observation('mq', 'e1', {'lag': 3, 'consumeEnabled': True})], SimpleNamespace(end=1000)))
    hit = result['cases'][0]
    assert '开启' in hit['differences'][0]
    assert hit['checks'][0]['observation_ids'] == ['e1']


def test_history_failure_preserves_refusal_and_fastpath_skips_retrieval():
    class BrokenHistory:
        reads = 0

        async def retrieve(self, *_):
            self.reads += 1
            raise TimeoutError

    async def run(fast):
        history = BrokenHistory()
        investigator = SimpleNamespace(history=history, settings=SimpleNamespace(configured=False))
        values = {
            'events': {'events': [], 'coverageComplete': True},
            'inventory': {'mysql': [{'stock': 9}], 'redisStock': '8' if fast else None, 'pending': [{'total': 1}]},
            'mq': {'lag': 1, 'brokerOffset': 2, 'consumerOffset': 1, 'consumeEnabled': False, 'brokers': 1},
            'delivery': {'events': [], 'orders': [], 'coverageComplete': True},
            'compensation': {},
            'traffic': {'events': [], 'coverageComplete': True},
        }
        observations, trace = [], []

        async def collect(tools, _):
            for tool in tools:
                observations.append(observation(tool, f'e{len(observations) + 1}', values[tool]))

        async def emit(event):
            trace.append(event)

        result, _ = await investigate(investigator, None, '核对当前请求', observations, collect, emit, emit)
        assert result['actions_executed'] is False
        if fast:
            assert history.reads == 0 and 'history' not in result
            assert result['root_code'] == 'MQ_CONSUME_DISABLED'
        else:
            assert history.reads == 1 and result['history']['strategy'] == 'unavailable'
            assert result['root_code'] == UNKNOWN
            assert set(result['evidence_ids']) <= {row['id'] for row in observations}

    asyncio.run(run(True))
    asyncio.run(run(False))


def test_refusal_cites_missing_inventory_and_other_limit():
    observations = [
        observation('events', 'e1', {'events': [{'status': 429, 'type': 'http', 'limitScope': 'user'}]}),
        observation('inventory', 'e2', {'mysql': [{'stock': 19}], 'redisStock': None}),
    ]
    result = inspect(observations, '确认限流维度')
    assert result['supported'][UNKNOWN]['evidence_ids'] == ['e1', 'e2']
    assert {item['kind'] for item in result['findings']} == {'other_limit', 'incomplete_inventory'}
