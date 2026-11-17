import asyncio
import json
import httpx

from core.config import Settings
from core.context import ToolContext
from core.engine import AgentEngine
from models.llm_client import open_model


def test_luna_uses_none_reasoning_for_chat_completions_tools():
    async def run():
        def model(request):
            body = json.loads(request.content)
            assert body["model"] == "gpt-6-luna"
            assert body["reasoning_effort"] == "none"
            assert body["tools"]
            return httpx.Response(200, json={"id": "fixture", "object": "chat.completion", "created": 1, "model": "gpt-6-luna",
                "choices": [{"index": 0, "message": {"role": "assistant", "content": "你好"}, "finish_reason": "stop"}]})
        async def emit(_):
            pass
        settings = Settings(api_key="local-fixture", model="gpt-6-luna")
        async with open_model(settings, httpx.MockTransport(model)) as client:
            answer = await AgentEngine(client, settings.model).run("你好", [], ToolContext(java=None), emit)
        assert answer == "你好"
    asyncio.run(run())
