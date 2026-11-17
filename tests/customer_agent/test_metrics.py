"""ASGI protocol load + real PostgreSQL; NOT a production HTTP/model latency claim."""
import asyncio
import json
import math
import os
from pathlib import Path
import time
import uuid

import httpx
from core.config import Settings
from main import create_app
from tests_support import TestMemory
from test_async_api import java


def test_100_submissions_keep_10_tasks_and_10_complete_turns():
    async def run():
        app = create_app(settings=Settings(api_key="", state_dsn=os.environ["AGENT_TEST_STATE_DSN"]),
                         transport=httpx.MockTransport(java), memory=TestMemory())
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local.test") as client:
                ids = [str(uuid.uuid4()) for _ in range(10)]
                async def submit(index):
                    started = time.perf_counter()
                    response = await client.post("/agent/tasks", headers={"authorization": "user-one"},
                        json={"request_id": ids[index % 10], "message": "查询店铺4详情"})
                    assert response.status_code == 202
                    return (time.perf_counter() - started) * 1000, response.json()
                results = await asyncio.gather(*(submit(index) for index in range(100)))
                # Shared consumers live for the application lifetime; await durable completion.
                deadline=asyncio.get_running_loop().time()+20
                while True:
                    unfinished=await app.state.task_store.call(lambda c:c.execute("SELECT COUNT(*) AS n FROM agent_tasks WHERE state IN ('accepted','running')").fetchone()['n'])
                    if not unfinished:break
                    assert asyncio.get_running_loop().time()<deadline
                    await asyncio.sleep(.02)
                store = app.state.task_store
                task_count = await store.call(lambda c: c.execute("SELECT COUNT(*) AS n FROM agent_tasks").fetchone()["n"])
                turns = sum(len(history) for history in app.state.memory.rows.values())
                assert task_count == 10 and turns == 10
                assert len({r[1]["task_id"] for r in results}) == 10
                durations = sorted(row[0] for row in results)
                def percentile(p):
                    return durations[math.ceil(len(durations) * p) - 1]
                report = {"tool": "HTTPX ASGITransport + asyncio.gather", "database": "real isolated PostgreSQL",
                          "java": "HTTP fixture", "llm": "zero calls, deterministic query", "memory": "in-memory test fixture",
                          "requests": 100, "unique_request_ids": 10, "tasks": task_count, "turns": turns,
                          "p50_ms": percentile(.5), "p95_ms": percentile(.95), "p99_ms": percentile(.99),
                          "latency_scope": "POST admission including auth fixture and PostgreSQL, excluding network and full answer"}
                path = Path(os.environ["PERSISTENCE_EVIDENCE"]) / "async-admission-metrics.json"
                path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    asyncio.run(run())
