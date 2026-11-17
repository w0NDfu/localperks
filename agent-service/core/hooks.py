"""Small lifecycle harness. Rules are explicit guardrails, not a safety classifier."""
import re
from dataclasses import dataclass

from core.state import AgentError


@dataclass(frozen=True)
class HookDecision:
    allowed: bool = True
    code: str = "ALLOW"
    message: str = ""


class AgentHook:
    async def before_request(self, message, ctx):
        return HookDecision()

    async def before_tool(self, tool, values, ctx):
        return HookDecision()


class BoundaryHook(AgentHook):
    # Only explicit override/exfiltration requests. Normal policy questions remain allowed.
    rules = (
        ("PROMPT_OVERRIDE", r"(?:忽略|绕过|跳过).{0,12}(?:系统提示|安全检查|权限检查|身份校验|本人确认)"),
        ("SECRET_REQUEST", r"(?:输出|泄露|打印|告诉我).{0,12}(?:系统提示词|API[_ ]?KEY|数据库密码|管理员令牌)"),
        ("CROSS_USER_ACCESS", r"(?:查询|查看|导出|取消).{0,8}(?:其他用户|别人的|他人的).{0,6}(?:订单|券包|聊天记录)"),
    )

    async def before_request(self, message, ctx):
        for code, pattern in self.rules:
            if re.search(pattern, message, re.IGNORECASE):
                return HookDecision(False, code, "该请求涉及权限或安全边界，请查询自己的订单，或联系人工客服。")
        return HookDecision()

    async def before_tool(self, tool, values, ctx):
        if tool.risk == "write":
            return HookDecision(False, "CONFIRMATION_REQUIRED", "交易工具必须通过本人确认流程执行")
        if tool.risk == "personal" and (not ctx.authenticated or not ctx.scope.startswith("user:")):
            return HookDecision(False, "LOGIN_REQUIRED", "请先登录后查询自己的券包或订单")
        if tool.risk not in {"read", "personal"}:
            return HookDecision(False, "TOOL_POLICY_DENIED", "工具未配置允许的风险等级")
        return HookDecision()


class Harness:
    def __init__(self, hooks=None):
        self.hooks = tuple(hooks) if hooks is not None else (BoundaryHook(),)

    async def check(self, stage, *, ctx, message=None, tool=None, values=None, emit=None):
        for hook in self.hooks:
            decision = (await hook.before_request(message, ctx) if stage == "request"
                        else await hook.before_tool(tool, values, ctx))
            if emit:
                await emit({"type": "harness", "stage": stage, "hook": type(hook).__name__,
                            "allowed": decision.allowed, "code": decision.code,
                            **({"tool": tool.name, "risk": tool.risk} if tool else {})})
            if not decision.allowed:
                status = 401 if decision.code == "LOGIN_REQUIRED" else 403
                raise AgentError(decision.code, decision.message, status)
