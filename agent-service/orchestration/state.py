from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class AgentState(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    WAITING_INPUT = "waiting_input"
    WAITING_CONFIRMATION = "waiting_confirmation"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    UNKNOWN = "unknown"


class Location(BaseModel):
    model_config = ConfigDict(extra="forbid")
    longitude: float = Field(ge=-180, le=180)
    latitude: float = Field(ge=-85.05112878, le=85.05112878)


class ConsumptionConstraints(BaseModel):
    model_config = ConfigDict(extra="forbid")
    location: Location | None = None
    budget_per_person: float | None = Field(default=None, gt=0, le=100000)
    people: int | None = Field(default=None, ge=1, le=50)
    visit_time: str | None = None
    tonight: bool = False
    radius_m: int = Field(default=5000, ge=100, le=5000)
    top_k: int = Field(default=3, ge=1, le=10)
    keyword: str = Field(default="", max_length=128)
    meal_only: bool = True
    cuisine_preferences: list[str] = Field(default_factory=list,max_length=8)
    excluded_categories: list[str] = Field(default_factory=list,max_length=8)
    answer_style: Literal['concise','detailed','neutral'] = 'neutral'

    @field_validator('cuisine_preferences','excluded_categories')
    @classmethod
    def valid_preferences(cls,value):
        from memory.profiles import UserPreferences
        return UserPreferences(cuisine_preferences=value).cuisine_preferences

    @field_validator('visit_time')
    @classmethod
    def valid_visit_time(cls, value):
        if value is not None:
            from datetime import datetime
            datetime.fromisoformat(value)
        return value


class Intent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["simple", "recommend", "knowledge", "action", "legacy"]
    tool: str | None = None
    agent: str | None = None
    arguments: dict[str, Any] = Field(default_factory=dict)
    constraints: ConsumptionConstraints | None = None


class TaskError(BaseModel):
    code: str
    message: str
    retryable: bool = False


class TaskResult(BaseModel):
    node_id: str
    agent: str
    status: Literal["completed", "failed", "skipped"]
    data: Any = None
    error: TaskError | None = None
    duration_ms: float = 0


class TaskNode(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(pattern=r"^[a-zA-Z0-9_:-]{1,100}$")
    agent: str
    operation: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    dependencies: list[str] = Field(default_factory=list)
    optional: bool = False
    volatile: bool = True
    timeout: float = Field(default=12, gt=0, le=60)


class TaskPlan(BaseModel):
    nodes: list[TaskNode]

    @model_validator(mode="after")
    def check_dag(self):
        from graphlib import TopologicalSorter, CycleError
        names = {node.id for node in self.nodes}
        if len(names) != len(self.nodes) or len(names) > 60:
            raise ValueError("节点ID必须唯一，且最多60个")
        if any(dep not in names for node in self.nodes for dep in node.dependencies):
            raise ValueError("依赖节点不存在")
        try:
            tuple(TopologicalSorter({n.id: n.dependencies for n in self.nodes}).static_order())
        except CycleError as exc:
            raise ValueError("任务依赖包含环") from exc
        return self
