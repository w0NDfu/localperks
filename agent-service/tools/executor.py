import asyncio
import json
import time

from pydantic import ValidationError

from orchestration.state import TaskError, TaskResult
from tools.java_api import ShopLinkError
from core.hooks import Harness
from core.state import AgentError


class ErrorHandler:
    @staticmethod
    def convert(exc: Exception) -> TaskError:
        if isinstance(exc, AgentError):
            return TaskError(code=exc.code, message=str(exc))
        if isinstance(exc, ShopLinkError):
            return TaskError(code=exc.code, message=str(exc),
                             retryable=exc.code in {"SHOPLINK_TIMEOUT", "SHOPLINK_UNAVAILABLE"})
        if isinstance(exc, (ValueError, TypeError, ValidationError)):
            return TaskError(code="INVALID_ARGUMENTS", message="工具参数不符合定义，请按工具schema修正")
        if isinstance(exc, TimeoutError):
            return TaskError(code="TOOL_TIMEOUT", message="工具执行超时", retryable=True)
        return TaskError(code="TOOL_FAILURE", message="工具执行异常，请稍后重试")


class ToolExecutor:
    def __init__(self, registry, concurrency=8, timeout=8, retries=1, harness=None):
        self.registry = registry
        self.limit = asyncio.Semaphore(concurrency)
        self.timeout = timeout
        self.retries = retries
        self.harness = harness or Harness()

    async def execute(self, agent, name, arguments, ctx, emit=None) -> TaskResult:
        started = time.perf_counter()
        tool = self.registry._tools.get(name)
        if tool is None or tool.agent != agent:
            return TaskResult(node_id=name, agent=agent, status="failed",
                              error=TaskError(code="UNKNOWN_TOOL", message="工具不在当前Agent权限范围"))
        try:
            payload = json.loads(arguments) if isinstance(arguments, str) else arguments
            values = tool.parameters.model_validate(payload)
            await self.harness.check("tool", ctx=ctx, tool=tool, values=values, emit=emit)
        except AgentError as exc:
            return TaskResult(node_id=name, agent=agent, status="failed", error=ErrorHandler.convert(exc))
        except (ValueError, TypeError, ValidationError) as exc:
            return TaskResult(node_id=name, agent=agent, status="failed", error=ErrorHandler.convert(exc))
        isolated = ctx.fork()
        if emit:
            await emit({"type": "tool_start", "agent": agent, "tool": name})
        attempts = 0
        async with self.limit:
            while True:
                try:
                    async with asyncio.timeout(self.timeout):
                        data = await tool.function(isolated, **values.model_dump())
                    if isinstance(data, dict) and data.get("error"):
                        result = TaskResult(node_id=name, agent=agent, status="failed", error=TaskError.model_validate(data["error"]))
                    else:
                        from tools.schemas import validate_result
                        validate_result(name, data)
                        result = TaskResult(node_id=name, agent=agent, status="completed", data=data)
                    break
                except Exception as exc:
                    error = ErrorHandler.convert(exc)
                    if tool.risk in {"read", "personal"} and error.retryable and attempts < self.retries:
                        attempts += 1
                        await asyncio.sleep(.1 * attempts)
                        continue
                    result = TaskResult(node_id=name, agent=agent, status="failed", error=error)
                    break
        result.duration_ms = round((time.perf_counter() - started) * 1000, 3)
        ctx.requests.extend(isolated.requests)
        if emit:
            await emit({"type": "tool_result", "agent": agent, "tool": name,
                        "requests": isolated.requests, "duration_ms": result.duration_ms,
                        "retries": attempts, "result": result.data if result.status == "completed" else {"error": result.error.model_dump()}})
        return result
