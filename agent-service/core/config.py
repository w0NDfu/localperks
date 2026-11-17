import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    api_key: str = field(repr=False)
    model: str = ""
    base_url: str = "https://api.openlux.ai/v1"
    shoplink_base_url: str = "http://127.0.0.1:8081"
    redis_url: str = field(default_factory=lambda: os.environ.get('REDIS_URL','redis://127.0.0.1:6379/0'), repr=False)
    business_timezone: str = "Asia/Shanghai"
    max_rounds: int = 5
    turn_timeout: int = 100
    orchestration_mode: str = "supervisor"
    state_dsn: str = field(default_factory=lambda: os.environ.get("AGENT_STATE_DSN", ""), repr=False)
    state_schema: str = field(default_factory=lambda: os.environ.get("AGENT_STATE_SCHEMA", "agent_state"))
    allow_mock_pay: bool = False
    ops_enabled: bool = False
    reasoning_effort: str | None = None
    pgvector_dsn: str = field(default="", repr=False)
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536
    rerank_model: str = "BAAI/bge-reranker-v2-m3"
    rerank_path: str = "/rerank"
    credential_key: str = field(default_factory=lambda: os.environ.get('AGENT_CREDENTIAL_KEY',''),repr=False)
    worker_concurrency: int = 16
    global_workflow_concurrency: int = 256
    queue_capacity: int = 10000
    per_user_pending: int = 32
    queue_wait_seconds: int = 300
    credential_ttl: int = 900
    model_rpm: int = 6000
    model_tpm: int = 6000000
    model_concurrency: int = 128
    model_output_tokens: int = 2048
    model_quota_wait: float = 2
    quota_scope: str = 'primary'
    redis_connections: int = 64
    redis_pool_wait: float = 10
    model_call_timeout: float = 45

    def __post_init__(self):
        for name in ('worker_concurrency','global_workflow_concurrency','queue_capacity','per_user_pending',
                     'queue_wait_seconds','credential_ttl','model_rpm','model_tpm','model_concurrency','model_output_tokens','redis_connections'):
            if getattr(self,name)<=0:
                raise ValueError(name+' must be positive')
        if self.credential_ttl < self.queue_wait_seconds + self.turn_timeout + 150:
            raise ValueError('Credential TTL must cover queue wait, workflow timeout and recovery lease')
        if not 0 <= self.model_quota_wait <= 30:
            raise ValueError('Model quota wait must be between 0 and 30 seconds')
        import math
        for name in ('redis_pool_wait','model_call_timeout'):
            if not math.isfinite(getattr(self,name)) or getattr(self,name)<=0:
                raise ValueError(name+' must be finite and positive')

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.model)


def load_settings() -> Settings:
    local = {**dotenv_values(PROJECT_ROOT / ".env.platform"), **dotenv_values(PROJECT_ROOT / ".env")}

    def value(name: str, default: str = "") -> str:
        return (os.environ.get(name, local.get(name)) or default).strip()

    if value("AGENT_TASK_DB"):
        raise ValueError("AGENT_TASK_DB 已移除；先迁移旧库，再改用 AGENT_STATE_DSN")

    return Settings(
        api_key=value("OPENAI_API_KEY"),
        model=value("OPENAI_MODEL"),
        base_url=value("OPENAI_BASE_URL", "https://api.openlux.ai/v1"),
        shoplink_base_url=value("LOCALPERKS_BASE_URL", value("SHOPLINK_BASE_URL", "http://127.0.0.1:8081")),
        redis_url=value("REDIS_URL", "redis://127.0.0.1:6379/0"),
        business_timezone=value("LOCALPERKS_TIMEZONE", value("SHOPLINK_TIMEZONE", "Asia/Shanghai")),
        orchestration_mode=value("AGENT_ORCHESTRATION", "supervisor"),
        state_dsn=value("AGENT_STATE_DSN", value("PGVECTOR_DSN")),
        state_schema=value("AGENT_STATE_SCHEMA", "agent_state"),
        allow_mock_pay=value("ALLOW_MOCK_PAY", "false").lower() == "true",
        ops_enabled=value("AGENT_OPS_ENABLED", "false").lower() == "true",
        reasoning_effort=value("OPENAI_REASONING_EFFORT") or ("none" if value("OPENAI_MODEL").startswith("gpt-6-luna") else None),
        pgvector_dsn=value("PGVECTOR_DSN"),
        embedding_model=value("EMBEDDING_MODEL", "text-embedding-3-small"),
        embedding_dimensions=int(value("EMBEDDING_DIMENSIONS", "1536")),
        rerank_model=value("RERANK_MODEL", "BAAI/bge-reranker-v2-m3"),
        rerank_path=value("RERANK_PATH", "/rerank"),
        credential_key=value('AGENT_CREDENTIAL_KEY') or (dotenv_values(PROJECT_ROOT/'.env.platform').get('AGENT_CREDENTIAL_KEY') or '').strip(),
        worker_concurrency=int(value('AGENT_WORKERS','16')),
        global_workflow_concurrency=int(value('AGENT_GLOBAL_WORKFLOWS','256')),
        queue_capacity=int(value('AGENT_QUEUE_CAPACITY','10000')),
        per_user_pending=int(value('AGENT_PER_USER_PENDING','32')),
        queue_wait_seconds=int(value('AGENT_QUEUE_WAIT_SECONDS','300')),
        credential_ttl=int(value('AGENT_CREDENTIAL_TTL','900')),
        model_rpm=int(value('AGENT_MODEL_RPM','6000')),
        model_tpm=int(value('AGENT_MODEL_TPM','6000000')),
        model_concurrency=int(value('AGENT_MODEL_CONCURRENCY','128')),
        model_output_tokens=int(value('AGENT_MODEL_OUTPUT_TOKENS','2048')),
        model_quota_wait=float(value('AGENT_MODEL_QUOTA_WAIT','2')),
        quota_scope=value('AGENT_QUOTA_SCOPE','primary'),
        redis_connections=int(value('AGENT_REDIS_CONNECTIONS','64')),
        redis_pool_wait=float(value('AGENT_REDIS_POOL_WAIT','10')),
        model_call_timeout=float(value('AGENT_MODEL_CALL_TIMEOUT','45')),
    )

