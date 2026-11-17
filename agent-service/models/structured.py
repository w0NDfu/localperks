import json
import time

from openai import APIConnectionError, APIStatusError, APITimeoutError

from core.state import AgentError
from models.llm_client import open_model
from models.runtime import output_limit


async def json_completion(settings, messages, emit, *, transport=None, purpose="interpretation"):
    started = time.perf_counter()
    await emit({"type": "model_request", "agent": "supervisor", "model": settings.model,
                "purpose": purpose, "step": 1})
    try:
        async with open_model(settings, transport) as client:
            response = await client.chat.completions.create(
                model=settings.model, messages=messages, response_format={"type": "json_object"}, max_completion_tokens=output_limit(),
                **({"reasoning_effort": settings.reasoning_effort} if settings.reasoning_effort else {}))
        if not response.choices or not response.choices[0].message.content:
            raise ValueError("Empty model response")
        output = json.loads(response.choices[0].message.content)
        if not isinstance(output, dict):
            raise ValueError("Expected JSON object")
    except APITimeoutError as exc:
        raise AgentError("MODEL_TIMEOUT", "模型请求超时", 504) from exc
    except APIConnectionError as exc:
        raise AgentError("MODEL_UNAVAILABLE", "无法连接模型中转站", 502) from exc
    except APIStatusError as exc:
        if getattr(exc,'code',None)=='agent_quota':
            raise AgentError('MODEL_QUOTA_BUSY','模型调用预算已满，请稍后重试',503) from exc
        raise AgentError("MODEL_HTTP_ERROR", "模型请求失败，请检查令牌、额度和模型能力", 502) from exc
    except (ValueError, TypeError) as exc:
        raise AgentError("MODEL_INVALID_JSON", "模型返回的结构化结果无效", 502) from exc
    await emit({"type": "model_response", "agent": "supervisor", "purpose": purpose,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                "usage": response.usage.model_dump() if response.usage else None})
    return output
