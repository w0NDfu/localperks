"""One real tool-calling request; never print credentials or provider response text."""
import asyncio
import json
from pathlib import Path
import sys
import time
import httpx

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "agent-service"))
from core.config import load_settings


async def main():
    settings = load_settings()
    started = time.perf_counter()
    async with httpx.AsyncClient(timeout=60, trust_env=False) as client:
        response = await client.post(settings.base_url.rstrip("/") + "/chat/completions",
            headers={"Authorization": "Bearer " + settings.api_key},
            json={"model": settings.model, "messages": [{"role": "user", "content": "请调用连接检查工具。"}],
                  "tools": [{"type": "function", "function": {"name": "check_connection", "description": "检查连接",
                             "parameters": {"type": "object", "properties": {}, "additionalProperties": False}}}],
                  "tool_choice": "auto", "reasoning_effort": "none"})
    result = {"model": settings.model, "status": response.status_code, "latency_ms": (time.perf_counter() - started) * 1000}
    if response.status_code == 200:
        payload = response.json()
        calls = payload["choices"][0]["message"].get("tool_calls", [])
        result.update(tool_calling=any(c["function"]["name"] == "check_connection" for c in calls), usage=payload.get("usage"))
    else:
        error = response.json().get("error", {})
        result["error"] = str(error.get("message", error)).replace(settings.api_key, "[REDACTED]")[:600]
    destination = ROOT / "artifacts/customer-assistant/model-smoke.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result), flush=True)
    assert result.get("tool_calling"), "Model connection/tool-calling check failed; provider text omitted"


if __name__ == "__main__":
    asyncio.run(main())
