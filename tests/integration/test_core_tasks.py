import asyncio
import json
import os
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx
import pytest

from decision.ranking import DecisionEngine
from orchestration.planner import Planner
from orchestration.router import Router
from orchestration.state import ConsumptionConstraints
from support import BusinessFixture, app_for, constraints, shop, voucher

DATASET = Path(__file__).resolve().parents[2] / 'evaluations/datasets/core_v03.jsonl'
CASES = [json.loads(line) for line in DATASET.read_text(encoding='utf-8').splitlines()]


@pytest.mark.parametrize('case', CASES, ids=lambda c: c['id'])
def test_core_task(case, tmp_path):
    started = time.perf_counter()
    record = {**case, 'test_type': 'deterministic_fixture', 'actual': None, 'trace': [],
              'success': False, 'failure_reason': None, 'model_requests': 0,
              'actual_provider_tokens': 0}
    try:
        if case['kind'] == 'router':
            result = Router().route(case['input'])
            record['actual'] = result.model_dump()
            assert result.kind == case['expected']['intent']
            if 'tool' in case['expected']:
                assert result.tool == case['expected']['tool']
        elif case['kind'] == 'constraint':
            result = Planner().constraints(case['input'], case.get('previous'))
            record['actual'] = result.model_dump()
            if 'missing' in case['expected']:
                assert set(Planner().missing(result)) == set(case['expected']['missing'])
            for key, value in case['expected'].items():
                if key != 'missing':
                    assert getattr(result, key) == value
        elif case['kind'] == 'decision':
            shops = [shop(**case.get('shop', {}))]
            offers = {1: [voucher(**case.get('voucher', {}))]} if not case.get('no_offers') else {1: []}
            opts = {**constraints(), **case.get('constraints', {})}
            result = DecisionEngine().rank(shops, offers, ConsumptionConstraints.model_validate(opts),
                                          now=datetime.now(ZoneInfo('Asia/Shanghai')))
            record['actual'] = result
            for field, number in case['expected'].items():
                if field == 'estimated_cash_cost':
                    assert result['recommendations'][0][field] == number
                else:
                    assert len(result[field]) == number
        else:
            async def check():
                business = BusinessFixture()
                if case.get('empty'):
                    business.shops = []
                business.failed_shop = case.get('failed_shop')
                app = app_for(os.environ['AGENT_TEST_STATE_DSN'], business)
                async with app.router.lifespan_context(app):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://fixture') as client:
                        body = {'message': case['input']}
                        if case.get('with_constraints'):
                            body['constraints'] = constraints()
                        response = await client.post('/agent/chat', json=body,
                                                     headers={'authorization': 'one'} if case.get('login') else {})
                        result = response.json()
                        record['actual'] = result
                        record['trace'] = result.get('trace', [])
                        assert response.status_code == case['expected'].get('http_status', 200), result
                        if response.status_code == 200:
                            assert result['state'] == case['expected']['state']
                            assert result['metrics']['model_calls'] == 0
                            if case['expected'].get('fast'):
                                assert not any(e['type'] == 'plan' for e in record['trace'])
                            if 'recommendations' in case['expected']:
                                assert len(result['decision']['recommendations']) == case['expected']['recommendations']
                            if case.get('failed_shop'):
                                assert result['decision']['warnings']
                            if case['expected'].get('no_write'):
                                assert business.writes == 0
                        return result
            asyncio.run(check())
        record['success'] = True
    except Exception as exc:
        record['failure_reason'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        record['duration_ms'] = (time.perf_counter() - started) * 1000
        directory = Path(os.environ['VALIDATION_RUN_DIR']) / 'cases'
        directory.mkdir(exist_ok=True)
        (directory / f"{case['id']}.json").write_text(json.dumps(record, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
