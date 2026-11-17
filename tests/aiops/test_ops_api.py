import os
import time
import asyncio
import sys
from pathlib import Path

from fastapi.testclient import TestClient

sys.path.insert(0,str(Path(__file__).resolve().parents[2] / 'agent-service'))
from core.config import Settings
from main import create_app


def test_admin_access_is_separate_from_consumer_credentials(monkeypatch, tmp_path):
    secret = 'test-only-admin-token-with-at-least-32-characters'
    monkeypatch.setenv('OPS_ADMIN_TOKEN', secret)
    monkeypatch.setenv('AGENT_OPS_ENABLED', 'true')
    with TestClient(create_app(settings=Settings(api_key='', ops_enabled=True, state_dsn=os.environ['AGENT_TEST_STATE_DSN']))) as client:
        path = '/agent/ops/access'
        assert client.get(path, headers={'Authorization': 'Bearer consumer-token'}).status_code == 403
        assert client.get(path, headers={'X-Ops-Token': b'\xe9'}).status_code == 403
        accepted = client.get(path, headers={'X-Ops-Token': secret})
        assert accepted.status_code == 200
        assert accepted.headers['Cache-Control'] == 'no-store'
        assert accepted.json()['read_only'] is True


def test_busy_admin_request_cannot_allocate_another_task(monkeypatch, tmp_path):
    secret = 'test-only-admin-token-with-at-least-32-characters'
    monkeypatch.setenv('OPS_ADMIN_TOKEN', secret)
    monkeypatch.setenv('OPS_JAVA_TOKEN', secret)
    app = create_app(settings=Settings(api_key='unit-only', model='unit-only', ops_enabled=True, state_dsn=os.environ['AGENT_TEST_STATE_DSN']))
    with TestClient(app) as client:
        app.state.ops_limit = asyncio.Semaphore(0)
        now = int(time.time() * 1000)
        payload = {'symptom':'库存异常', 'scope':{'start':now-500, 'end':now, 'voucher_id':1}}
        headers = {'X-Ops-Token':secret}
        assert client.post('/agent/ops/investigations', json=payload, headers=headers).status_code == 429
        assert client.get('/agent/ops/investigations', headers=headers).json() == {'items':[]}
