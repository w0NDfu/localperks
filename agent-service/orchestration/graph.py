import asyncio
import time

from orchestration.state import TaskError, TaskPlan, TaskResult


class TaskGraph:
    def __init__(self, concurrency=4):
        self.limit = asyncio.Semaphore(concurrency)

    async def run(self, plan: TaskPlan, handler, *, emit=None, checkpoint=None, restored=None):
        results: dict[str, TaskResult] = {}
        restored = restored or {}
        pending = {node.id: node for node in plan.nodes}

        async def execute(node):
            previous = restored.get(node.id)
            if previous and previous.status == "completed" and not node.volatile:
                if emit:
                    await emit({'type': 'checkpoint_restored', 'node_id': node.id, 'agent': node.agent})
                return previous
            bad = [dep for dep in node.dependencies if results[dep].status != "completed"]
            if bad and not node.optional:
                result = TaskResult(node_id=node.id, agent=node.agent, status="skipped",
                                    error=TaskError(code="DEPENDENCY_FAILED", message="依赖任务未成功"))
                if checkpoint:
                    await checkpoint(node, result)
                if emit:
                    await emit({'type': 'node_result', **result.model_dump()})
                return result
            started = time.perf_counter()
            async with self.limit:
                if emit:
                    await emit({"type": "node_start", "node_id": node.id, "agent": node.agent})
                try:
                    async with asyncio.timeout(node.timeout):
                        output = await handler(node, results)
                    result = output if isinstance(output, TaskResult) else TaskResult(
                        node_id=node.id, agent=node.agent, status="completed", data=output)
                    result.node_id = node.id
                except TimeoutError:
                    result = TaskResult(node_id=node.id, agent=node.agent, status="failed",
                                        error=TaskError(code="NODE_TIMEOUT", message="任务节点超时", retryable=True))
                except Exception as exc:
                    from tools.executor import ErrorHandler
                    result = TaskResult(node_id=node.id, agent=node.agent, status="failed", error=ErrorHandler.convert(exc))
            result.duration_ms = round((time.perf_counter() - started) * 1000, 3)
            if checkpoint:
                await checkpoint(node, result)
            if emit:
                await emit({"type": "node_result", **result.model_dump()})
            return result

        while pending:
            ready = [node for node in pending.values() if all(dep in results for dep in node.dependencies)]
            if not ready:
                raise ValueError("Invalid DAG: no runnable node")
            # The scheduler starts every ready branch together, bounded by self.limit.
            completed = await asyncio.gather(*(execute(node) for node in ready))
            for node, result in zip(ready, completed):
                results[node.id] = result
                del pending[node.id]
        return results
