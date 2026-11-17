"""Runs the frozen pre-upgrade service in a separate interpreter/import namespace."""
import asyncio
import json
import sys
import tempfile
import time
import zipfile
from pathlib import Path


def execute(archive, message):
    with tempfile.TemporaryDirectory(prefix='localperks-baseline-') as extracted:
        with zipfile.ZipFile(archive) as bundle:
            for entry in bundle.infolist():
                target = (Path(extracted) / entry.filename).resolve()
                if not target.is_relative_to(Path(extracted).resolve()):
                    raise ValueError('Unsafe baseline archive')
            bundle.extractall(extracted)
        sys.path.insert(0, extracted)
        from main import create_app
        import httpx
        async def run():
            app = create_app()
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url='http://baseline', timeout=115) as client:
                    started = time.perf_counter()
                    response = await client.post('/agent/chat/stream', json={'message': message})
                    elapsed = (time.perf_counter() - started) * 1000
            events = [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith('data: ')]
            completed = next((event for event in reversed(events) if event.get('type') == 'done'), None)
            failure = next((event for event in reversed(events) if event.get('type') == 'error'), None)
            data = completed or {'error': failure or {'code': 'INCOMPLETE_BASELINE'},
                                 'trace': [event for event in events if event.get('type') not in {'session', 'done'}]}
            data.update(http_status=response.status_code, measured_latency_ms=elapsed)
            return data
        return asyncio.run(run())


if __name__ == '__main__':
    payload = json.load(sys.stdin)
    try:
        result = execute(payload['archive'], payload['message'])
    except Exception as exc:
        result = {'http_status': 0, 'error': type(exc).__name__}
    print(json.dumps(result, ensure_ascii=False))
