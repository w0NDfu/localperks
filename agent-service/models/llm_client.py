from contextlib import asynccontextmanager

import httpx
from openai import AsyncOpenAI

from core.config import Settings
from models.runtime import ACTIVE_RUNTIME, QuotaTransport


@asynccontextmanager
async def open_model(settings: Settings, transport: httpx.AsyncBaseTransport | None = None):
    runtime = ACTIVE_RUNTIME.get()
    if runtime and runtime.settings.base_url==settings.base_url and transport is runtime.original_transport:
        yield runtime.client
        return
    if runtime:
        transport = QuotaTransport(transport or httpx.AsyncHTTPTransport(retries=0),runtime.quota)
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(35, connect=8),
        transport=transport,
        trust_env=False,
    ) as http_client:
        async with AsyncOpenAI(
            api_key=settings.api_key,
            base_url=settings.base_url,
            max_retries=0,
            http_client=http_client,
        ) as client:
            yield client

