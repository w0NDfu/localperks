"""Reproducible local harness metrics, with explicit attack and benign denominators."""
import asyncio
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "agent-service"))
from core.hooks import Harness
from core.context import ToolContext
from core.state import AgentError
from tools.registry import ToolRegistry
from tools.executor import ToolExecutor


async def main():
    dataset = json.loads((ROOT / "evaluations/harness_cases.json").read_text(encoding="utf-8"))
    rows = []
    for expected in ("reject", "allow"):
        for message in dataset[expected]:
            code = "ALLOW"
            try:
                await Harness().check("request", ctx=None, message=message)
            except AgentError as exc:
                code = exc.code
            rows.append({"stage": "request", "message": message, "expected": expected, "blocked": code != "ALLOW", "code": code})
    registry, writes = ToolRegistry(), []
    @registry.tool(agent="evaluation", description="private fixture", risk="personal")
    async def personal(ctx):
        return {"records": []}
    @registry.tool(agent="evaluation", description="write fixture", risk="write")
    async def transaction(ctx, value: int):
        writes.append(value)
        return {"ok": True}
    executor = ToolExecutor(registry)
    for index in range(12):
        tool = "transaction" if index % 2 else "personal"
        ctx = ToolContext(java=None, authenticated=tool == "transaction", scope="user:1" if tool == "transaction" else "guest")
        result = await executor.execute("evaluation", tool, {"value": index} if tool == "transaction" else {}, ctx)
        rows.append({"stage": "tool", "tool": tool, "expected": "reject", "blocked": result.status == "failed", "code": result.error.code if result.error else "ALLOW"})
    assert not writes
    malicious = [r for r in rows if r["expected"] == "reject"]
    benign = [r for r in rows if r["expected"] == "allow"]
    report = {"dataset": dataset["version"], "scope": dataset["scope"], "malicious": len(malicious), "benign": len(benign),
              "blocked": sum(r["blocked"] for r in malicious), "false_positives": sum(r["blocked"] for r in benign),
              "interception_rate": sum(r["blocked"] for r in malicious) / len(malicious),
              "false_positive_rate": sum(r["blocked"] for r in benign) / len(benign), "model_requests": 0, "rows": rows}
    path = ROOT / "artifacts/customer-assistant/harness.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in report.items() if k != "rows"}, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
