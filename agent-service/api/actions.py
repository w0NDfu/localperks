import hashlib
import hmac
import json
import secrets
import time
import uuid
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.state import AgentError
from memory.task_store import dump
from tools.java_api import ShopLinkError


class ActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    operation: Literal["seckill", "cancel", "mock_pay"]
    voucher_id: int | None = Field(default=None, ge=1)
    order_id: str | None = Field(default=None, pattern=r"^[0-9]{1,19}$")
    idempotency_key: uuid.UUID = Field(default_factory=uuid.uuid4)

    @model_validator(mode="after")
    def target(self):
        if self.operation == "seckill":
            if self.voucher_id is None or self.order_id is not None:
                raise ValueError("抢券仅接受voucher_id")
        elif self.order_id is None or self.voucher_id is not None or int(self.order_id) > 9223372036854775807:
            raise ValueError("订单操作仅接受有效order_id")
        return self


class ActionConfirmation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    confirmed: Literal[True]
    confirmation_token: str = Field(min_length=20, max_length=200)


class ActionService:
    def __init__(self, store, settings):
        self.store, self.settings = store, settings

    async def log(self, action_id, event):
        await self.store.call(lambda c: c.execute('INSERT INTO agent_action_events(action_id,event,created) VALUES(%s,%s,%s)',
                                                  (action_id, dump(event), time.time())))

    async def events(self, action_id, ctx):
        await self.get(action_id, ctx)
        rows = await self.store.call(lambda c: c.execute('SELECT seq,event,created FROM agent_action_events WHERE action_id=%s ORDER BY seq', (action_id,)).fetchall())
        return [{'seq': r['seq'], 'created': r['created'], **json.loads(r['event'])} for r in rows]

    @staticmethod
    def require_user(ctx):
        if not ctx.authenticated or not ctx.scope.startswith("user:"):
            raise AgentError("LOGIN_REQUIRED", "交易操作必须先登录", 401)

    async def preview(self, request, ctx):
        self.require_user(ctx)
        if request.operation == "mock_pay" and not self.settings.allow_mock_pay:
            raise AgentError("MOCK_PAY_DISABLED", "Agent模拟支付默认禁用；系统尚未接入真实支付通道", 403)
        if request.operation == "seckill":
            rows = await ctx.java.get("/voucher/market")
            row = next((v for v in rows if v.get("id") == request.voucher_id), None)
            if row is None or row.get("type") != 1:
                raise AgentError("VOUCHER_UNAVAILABLE", "未找到可购买的秒杀券", 409)
            from datetime import datetime
            from zoneinfo import ZoneInfo
            from decision.ranking import parse_time
            zone = ZoneInfo(ctx.business_timezone)
            now = datetime.now(zone)
            try:
                active = parse_time(row["beginTime"], zone) <= now < parse_time(row["endTime"], zone)
            except (KeyError, ValueError, TypeError):
                active = False
            if not active or not isinstance(row.get("stock"), int) or row["stock"] <= 0:
                raise AgentError("VOUCHER_UNAVAILABLE", "活动未开始、已结束或实时库存不足/未知", 409)
            return {"operation": "seckill", "voucher_id": request.voucher_id, "title": row.get("title"),
                    "pay_cents": row.get("payValue"), "face_cents": row.get("actualValue"),
                    "rules": row.get("rules"), "begin_time": row.get("beginTime"), "end_time": row.get("endTime"),
                    "notice": "确认仅提交抢券请求，不会自动付款，成交资格由Java校验"}
        result = await ctx.java.get(f"/voucher-order/status/{request.order_id}", authenticated=True)
        if result.get("statusCode") not in (12, 14 if request.operation == "cancel" else 13):
            raise AgentError("ORDER_STATE_CONFLICT", "订单当前状态不允许该操作", 409)
        return {"operation": request.operation, "order_id": request.order_id,
                "notice": "取消未支付订单" if request.operation == "cancel" else "仅开发模拟支付，不会实际扣款"}

    async def prepare(self, request, ctx):
        self.require_user(ctx)
        payload = request.model_dump(mode="json", exclude={"idempotency_key"})
        existing = await self.store.call(lambda c: c.execute(
            "SELECT * FROM agent_actions WHERE scope=%s AND idem=%s", (ctx.scope, str(request.idempotency_key))).fetchone())
        if existing:
            if json.loads(existing["payload"]) != payload:
                raise AgentError("IDEMPOTENCY_CONFLICT", "相同幂等键不能用于不同操作", 409)
            return self.public(existing)
        try:
            preview = await self.preview(request, ctx)
        except ShopLinkError as exc:
            raise AgentError(exc.code, str(exc), exc.status_code) from exc
        action_id, nonce, now = str(uuid.uuid4()), secrets.token_urlsafe(32), time.time()
        def insert(c):
            c.execute("INSERT INTO agent_actions VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(scope,idem) DO NOTHING",
                      (action_id, ctx.scope, str(request.idempotency_key), request.operation,
                       dump(payload), dump(preview), nonce, "prepared", None, now, now))
            return c.execute("SELECT * FROM agent_actions WHERE scope=%s AND idem=%s", (ctx.scope, str(request.idempotency_key))).fetchone()
        stored = await self.store.call(insert)
        if json.loads(stored["payload"]) != payload:
            raise AgentError("IDEMPOTENCY_CONFLICT", "相同幂等键不能用于不同操作", 409)
        await self.log(stored['id'], {'type': 'action_prepared', 'operation': request.operation, 'preview': preview})
        return self.public(stored)

    def public(self, row):
        return {"action_id": row["id"], "operation": row["operation"], "state": row["state"],
                "preview": json.loads(row["preview"]), "confirmation_token": row["nonce"] if row["state"] == "prepared" else None,
                "expires_at": row["created"] + 600,
                "result": json.loads(row["result"]) if row["result"] else None}

    async def get(self, action_id, ctx):
        self.require_user(ctx)
        row = await self.store.call(lambda c: c.execute(
            "SELECT * FROM agent_actions WHERE id=%s AND scope=%s", (action_id, ctx.scope)).fetchone())
        if row is None:
            raise AgentError("ACTION_NOT_FOUND", "操作不存在或不属于当前用户", 404)
        return row

    async def confirm(self, action_id, confirmation, ctx):
        row = await self.get(action_id, ctx)
        if not hmac.compare_digest(row["nonce"], confirmation.confirmation_token):
            raise AgentError("CONFIRMATION_INVALID", "确认凭证无效", 403)
        if row["state"] == "completed":
            return self.public(row)
        if row["state"] != "prepared":
            raise AgentError("ACTION_RECONCILE_REQUIRED", "该请求可能已提交，请查询核对结果，不要重复提交", 409)
        if time.time() - row["created"] > 600:
            raise AgentError("CONFIRMATION_EXPIRED", "确认已过期，请重新核对并创建操作", 409)
        request = ActionRequest.model_validate({**json.loads(row["payload"]), "idempotency_key": row["idem"]})
        try:
            current = await self.preview(request, ctx)
        except ShopLinkError as exc:
            raise AgentError(exc.code, str(exc), exc.status_code) from exc
        if current != json.loads(row["preview"]):
            raise AgentError("PREVIEW_CHANGED", "价格或活动规则已变化，请重新创建并确认操作", 409)
        changed = await self.store.call(lambda c: c.execute(
            "UPDATE agent_actions SET state='running',updated=%s WHERE id=%s AND scope=%s AND state='prepared'",
            (time.time(), action_id, ctx.scope)).rowcount)
        if not changed:
            raise AgentError("ACTION_BUSY", "操作已经提交，请查询结果", 409)
        await self.log(action_id, {'type': 'submission_started', 'operation': request.operation})
        operation = request.operation
        path = (f"/voucher-order/seckill/{request.voucher_id}" if operation == "seckill" else
                f"/voucher-order/cancel/{request.order_id}" if operation == "cancel" else
                f"/voucher-order/mock-pay/{request.order_id}/true")
        try:
            result = await ctx.java.post(path, evidence=ctx.requests)
            state = "completed"
        except ShopLinkError as exc:
            state = "failed" if exc.code in {"SHOPLINK_BUSINESS_ERROR", "LOGIN_REQUIRED", "SHOPLINK_RATE_LIMITED"} else "unknown"
            result = {"error": {"code": exc.code, "message": str(exc)},
                      "notice": "结果不明时仅允许查询核对，禁止自动重试写操作"}
        # Cancellation/crash after claiming leaves 'running'; reconciliation never repeats POST.
        await self.store.call(lambda c: c.execute("UPDATE agent_actions SET state=%s,result=%s,updated=%s WHERE id=%s",
                                                   (state, dump(result), time.time(), action_id)))
        await self.log(action_id, {'type': 'submission_result', 'state': state, 'result': result, 'requests': ctx.requests})
        return self.public(await self.get(action_id, ctx))

    async def reconcile(self, action_id, ctx):
        row = await self.get(action_id, ctx)
        if row["state"] not in {"running", "unknown"}:
            return self.public(row)
        payload, evidence = json.loads(row["payload"]), None
        try:
            if row["operation"] == "seckill":
                for page in range(1, 6):
                    wallet = await ctx.java.get("/voucher-order/mine", params={"page": page, "ownedOnly": "false"}, authenticated=True)
                    records = wallet.get("records", [])
                    evidence = next((r for r in records if r.get("voucherId") == payload["voucher_id"]), None)
                    if evidence or len(records) < 12:
                        break
            else:
                result = await ctx.java.get(f"/voucher-order/status/{payload['order_id']}", authenticated=True)
                target = 14 if row["operation"] == "cancel" else 13
                evidence = result if result.get("statusCode") == target else None
        except ShopLinkError as exc:
            await self.log(action_id, {'type': 'recovery_failed', 'code': exc.code})
            raise AgentError(exc.code, str(exc), exc.status_code) from exc
        if evidence:
            await self.store.call(lambda c: c.execute("UPDATE agent_actions SET state='completed',result=%s,updated=%s WHERE id=%s",
                                                       (dump({"reconciled": True, "evidence": evidence}), time.time(), action_id)))
        await self.log(action_id, {'type': 'reconciliation', 'resolved': bool(evidence), 'evidence': evidence})
        return self.public(await self.get(action_id, ctx))
