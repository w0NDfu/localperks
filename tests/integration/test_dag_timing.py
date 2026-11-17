import asyncio
import json
import os
import time
from pathlib import Path

from orchestration.graph import TaskGraph
from orchestration.state import TaskNode, TaskPlan


def test_independent_dag_branches_overlap_and_join_sees_all_results():
    async def check():
        active = 0
        peak = 0
        async def work(node, results):
            nonlocal active, peak
            if node.id == 'join':
                assert all(results[key].status == 'completed' for key in ('a', 'b', 'c'))
                return 'joined'
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(.02)
            active -= 1
            return node.id
        nodes = [TaskNode(id=key, agent='voucher', operation='fixture') for key in ('a', 'b', 'c')]
        started = time.perf_counter()
        for node in nodes:
            await work(node, {})
        sequential_ms = (time.perf_counter() - started) * 1000
        peak = 0
        started = time.perf_counter()
        plan = TaskPlan(nodes=nodes + [TaskNode(id='join', agent='supervisor', operation='fixture', dependencies=['a', 'b', 'c'])])
        result = await TaskGraph().run(plan, work)
        parallel_ms = (time.perf_counter() - started) * 1000
        assert peak == 3 and result['join'].data == 'joined'
        # Overlap is asserted by active branch count, not a flaky speed threshold.
        (Path(os.environ['VALIDATION_RUN_DIR']) / 'dag_timing.json').write_text(json.dumps({
            'test_type': 'synthetic_three_branches_each_async_sleep_20ms', 'peak_active_branches': peak,
            'sequential_ms': sequential_ms, 'dag_parallel_ms': parallel_ms,
            'note': 'Scheduler mechanics only; not business API or real-model speedup.'}, indent=2))
    asyncio.run(check())
