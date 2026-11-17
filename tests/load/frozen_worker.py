"""Isolated import namespace for the immutable v0.2 ZIP; no business network."""
import argparse
import asyncio
import json
import os
import sys
import tempfile
import time
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
AGENT = ROOT / 'agent-service'


async def execute(args):
    with tempfile.TemporaryDirectory(dir=Path(args.output) / 'temp', prefix='baseline-') as temporary:
        extracted = Path(temporary).resolve()
        with zipfile.ZipFile(AGENT / 'evaluations/baseline_v02.zip') as archive:
            for entry in archive.infolist():
                if not (extracted / entry.filename).resolve().is_relative_to(extracted):
                    raise ValueError('Unsafe archive')
            archive.extractall(extracted)
        sys.path[:0] = [str(extracted), str(ROOT / 'tests'), str(AGENT)]
        import httpx
        from core.config import Settings
        from main import create_app
        from support import BusinessFixture
        from tests_support import TestMemory
        # Import helper after frozen modules are cached, preserving module identity.
        from validation_workload import BudgetTransport, mock_model
        cfg = Settings(api_key=os.environ.get('OPENAI_API_KEY', 'fixture') if args.real else 'fixture',
                       model=os.environ.get('OPENAI_MODEL', 'mock') if args.real else 'mock',
                       base_url=os.environ.get('OPENAI_BASE_URL', 'https://fixture/v1'))
        app = create_app(settings=cfg, memory=TestMemory(), transport=httpx.MockTransport(BusinessFixture()),
                         model_transport=BudgetTransport.create(Path(args.output)) if args.real else httpx.MockTransport(mock_model))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app, raise_app_exceptions=False), base_url='http://fixture') as client:
                started = time.perf_counter()
                response = await client.post('/agent/chat', json={'message': args.message})
                try:
                    result = response.json()
                except ValueError:
                    result = {'error': 'INVALID_RESPONSE'}
                result.update(http_status=response.status_code, measured_latency_ms=(time.perf_counter() - started) * 1000)
                return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--message', required=True)
    parser.add_argument('--real', action='store_true')
    try:
        print(json.dumps(asyncio.run(execute(parser.parse_args())), ensure_ascii=False))
    except Exception as exc:
        print(json.dumps({'http_status': 0, 'error': type(exc).__name__, 'measured_latency_ms': 0}))
        raise
