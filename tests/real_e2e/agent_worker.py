import argparse
import asyncio
import json
import os
import sys
import tempfile
import time
import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
AGENT = ROOT / 'agent-service'
sys.path.insert(0, str(HERE))
from model_budget import RealBudgetTransport


async def baseline(run, message):
    with tempfile.TemporaryDirectory(dir=run / 'temp', prefix='frozen-real-') as directory:
        extracted = Path(directory).resolve()
        with zipfile.ZipFile(AGENT / 'evaluations/baseline_v02.zip') as archive:
            for entry in archive.infolist():
                if not (extracted / entry.filename).resolve().is_relative_to(extracted):
                    raise ValueError('Unsafe frozen archive')
            archive.extractall(extracted)
        sys.path[:0] = [str(extracted)]
        from core.config import load_settings
        from main import create_app
        from dataclasses import replace
        import httpx
        # Frozen source has no .env. The parent supplies current model settings via process env.
        settings = replace(load_settings(), turn_timeout=180)
        app = create_app(settings=settings, model_transport=RealBudgetTransport(run))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app, raise_app_exceptions=False), base_url='http://frozen') as client:
                start = time.perf_counter()
                response = await client.post('/agent/chat/stream', json={'message': message})
                events = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith('data: ')]
                data = next((e for e in reversed(events) if e.get('type') == 'done'),
                            {'error': next((e for e in events if e.get('type') == 'error'), 'INCOMPLETE'), 'trace': events})
                data.update(http_status=response.status_code, duration_ms=(time.perf_counter() - start) * 1000)
                return data


def server(run, port):
    sys.path.insert(0, str(AGENT))
    from core.config import load_settings
    from main import create_app
    from dataclasses import replace
    import uvicorn
    cfg = replace(load_settings(), turn_timeout=180)
    app = create_app(settings=cfg, model_transport=RealBudgetTransport(run))
    uvicorn.run(app, host='127.0.0.1', port=port, log_level='warning')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['baseline', 'server'])
    parser.add_argument('--run', required=True)
    parser.add_argument('--port', type=int)
    parser.add_argument('--message')
    args = parser.parse_args()
    if args.mode == 'server':
        server(Path(args.run), args.port)
    else:
        try:
            print(json.dumps(asyncio.run(baseline(Path(args.run), args.message)), ensure_ascii=False))
        except Exception as error:
            print(json.dumps({'http_status': 0, 'error': type(error).__name__, 'duration_ms': 0}))
