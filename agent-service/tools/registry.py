import inspect
import json
from dataclasses import dataclass
from typing import get_type_hints

from pydantic import BaseModel, ConfigDict, ValidationError, create_model

from core.context import ToolContext
from tools.java_api import ShopLinkError


@dataclass
class RegisteredTool:
    name: str
    agent: str
    description: str
    parameters: type[BaseModel]
    function: object
    risk: str = "read"

    def schema(self) -> dict:
        return {"type": "function", "function": {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters.model_json_schema(),
        }}


class ToolRegistry:
    def __init__(self):
        self._tools: dict[str, RegisteredTool] = {}

    def tool(self, *, agent: str, description: str, risk: str = "read"):
        def register(function):
            if function.__name__ in self._tools:
                raise ValueError("Duplicate tool registration")
            hints = get_type_hints(function, include_extras=True)
            fields = {}
            for name, parameter in inspect.signature(function).parameters.items():
                if name == "ctx":
                    continue
                default = ... if parameter.default is inspect.Parameter.empty else parameter.default
                fields[name] = (hints[name], default)
            model = create_model(
                function.__name__ + "Arguments",
                __config__=ConfigDict(extra="forbid", strict=True),
                **fields,
            )
            self._tools[function.__name__] = RegisteredTool(
                function.__name__, agent, description, model, function, risk
            )
            return function
        return register

    def schemas(self, agent: str) -> list[dict]:
        return [tool.schema() for tool in self._tools.values() if tool.agent == agent]

    async def execute(self, agent: str, name: str, arguments: str, ctx: ToolContext) -> dict:
        from tools.executor import ToolExecutor
        result = await ToolExecutor(self).execute(agent, name, arguments, ctx)
        return result.data if result.status == "completed" else {"error": result.error.model_dump(exclude={"retryable"})}


registry = ToolRegistry()

