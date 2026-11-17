import asyncio
import json
import sys
from pathlib import Path

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'load'))
from validation_workload import BudgetTransport


@pytest.mark.parametrize('mode', ['token_limit', 'request_limit', 'missing_usage', 'unverified_accounting', 'destination'])
def test_paid_request_budget_stops_before_next_request(tmp_path, monkeypatch, mode):
    async def check():
        calls = []
        async def send(self, url, **kwargs):
            calls.append(1)
            usage = {} if mode == 'missing_usage' else {'total_tokens': 9000 if mode == 'unverified_accounting' else 6000}
            return httpx.Response(200, json={'usage': usage})
        monkeypatch.setattr(httpx.AsyncClient, 'post', send)
        monkeypatch.setenv('OPENAI_MAX_REQUESTS', '1' if mode == 'request_limit' else '40')
        monkeypatch.setenv('OPENAI_MAX_TOKENS_BUDGET', '24000')
        transport = BudgetTransport.create(tmp_path)
        async def invoke():
            return await transport.handle_async_request(httpx.Request('POST',
                'https://unapproved.example/v1/chat/completions' if mode == 'destination' else 'https://api.openlux.ai/v1/chat/completions',
                headers={'authorization': 'Bearer fixture'}, json={'messages': [{'role': 'user', 'content': 'fixture'}]}))
        successes = 3 if mode == 'token_limit' else 0 if mode == 'destination' else 1
        for _ in range(successes):
            await invoke()
        with pytest.raises(RuntimeError):
            await invoke()
        assert len(calls) == successes
        if successes:
            ledger = json.loads((tmp_path / 'model_budget.json').read_text())
            assert ledger['actual_tokens'] <= 24000 and ledger['requests'] == successes
    asyncio.run(check())
