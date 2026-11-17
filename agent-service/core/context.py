from dataclasses import dataclass, field

from tools.java_api import JavaAPI


@dataclass
class ToolContext:
    java: JavaAPI = field(repr=False)
    authenticated: bool = False
    business_timezone: str = "Asia/Shanghai"
    requests: list[dict] = field(default_factory=list)
    scope: str = "guest"
    rag: object | None = field(default=None, repr=False)
    preferences: dict = field(default_factory=dict)

    def fork(self):
        return ToolContext(java=self.java, authenticated=self.authenticated,
                           business_timezone=self.business_timezone, scope=self.scope, rag=self.rag,
                           preferences=dict(self.preferences))

