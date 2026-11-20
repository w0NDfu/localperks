import json
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from agents.business import OrderAgent, ShopAgent, VoucherAgent
from core.engine import AgentEngine
from core.state import AgentError
from decision.ranking import DecisionEngine
from models.llm_client import open_model
from models.structured import json_completion
from orchestration.graph import TaskGraph
from orchestration.planner import Planner
from orchestration.router import Router
from orchestration.state import AgentState, TaskNode, TaskPlan
from tools.executor import ToolExecutor
from tools.registry import registry
from models.grounded import compose
from orchestration.dialogue import interpret, decision_facts, decision_context, followup


class Interpretation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    keyword: str = Field(default="", max_length=128)
    explanation: str = Field(default="", max_length=800)


class DecisionExplanation(BaseModel):
    model_config = ConfigDict(extra='forbid')
    focus: Literal['savings', 'distance', 'budget', 'uncertainty']
    mentioned_shop_ids: list[int] = Field(default_factory=list, max_length=10)


class Supervisor:
    def __init__(self, settings, model_transport=None, *, store=None, task_id=None):
        self.settings, self.model_transport = settings, model_transport
        self.router, self.planner = Router(), Planner()
        self.executor = ToolExecutor(registry)
        self.shop, self.voucher, self.order = ShopAgent(self.executor), VoucherAgent(self.executor), OrderAgent(self.executor)
        self.store, self.task_id = store, task_id

    async def run(self, message, history, ctx, emit, constraints=None):
        from memory.profiles import answer_style
        previous=next((turn.get('constraints') for turn in reversed(history) if turn.get('constraints')),None)
        effective={**{k:v for k,v in getattr(ctx,'preferences',{}).items() if v is not None},**(previous or {}),**(constraints or {})}
        parsed=self.planner.constraints(message,previous,effective,ctx.business_timezone)
        with answer_style(parsed.answer_style):
            return await self._run(message,history,ctx,emit,effective)

    async def _run(self, message, history, ctx, emit, constraints=None):
        previous = next((turn.get("constraints") for turn in reversed(history) if turn.get("constraints")), None)
        import re
        context = next((turn.get('decision_context') for turn in reversed(history) if turn.get('decision_context')), None)
        if context and self.settings.configured and re.search(r'第[一二两三四五六七八九十\d]+家|这家|那家|上一家|确定.*(用|使用)|今晚.*(用|使用)', message):
            await emit({'type':'route','intent':'followup','strategy':'context_reference'})
            return await followup(self,message,history,context,previous,ctx,emit)
        intent = self.router.route(message, previous or constraints)
        await emit({"type": "route", "intent": intent.kind, "agent": intent.agent,
                    "tool": intent.tool, "strategy": "direct" if intent.kind == "simple" else intent.kind})
        if intent.kind == "simple":
            if intent.tool == "track_order":
                data = await self.order.track(intent.arguments["order_id"], ctx, emit)
                return {"answer": self.describe("order_status", data), "state": AgentState.COMPLETED,
                        "intent": "simple", "data": data}
            result = await self.executor.execute(intent.agent, intent.tool, intent.arguments, ctx, emit)
            if result.status != "completed":
                return {"answer": result.error.message, "state": AgentState.FAILED,
                        "intent": intent.kind, "error": result.error.model_dump()}
            return {"answer": self.describe(intent.tool, result.data), "state": AgentState.COMPLETED,
                    "intent": intent.kind, "data": result.data}
        if intent.kind == "recommend":
            parsed = self.planner.constraints(message, previous, constraints, ctx.business_timezone)
            explanation = "根据预算、距离、实时库存和明确的使用规则进行计算。"
            if self.settings.configured:
                try:
                    import hashlib
                    fingerprint = hashlib.sha256(json.dumps({'message':message,'constraints':parsed.model_dump()},sort_keys=True).encode()).hexdigest()
                    saved = await self.store.checkpoints(ctx.scope,self.task_id) if self.store and self.task_id else {}
                    checkpoint = saved.get('interpretation',{}).get('data',{})
                    if checkpoint.get('fingerprint') == fingerprint and checkpoint.get('parsed_constraints'):
                        from orchestration.state import ConsumptionConstraints
                        parsed = ConsumptionConstraints.model_validate(checkpoint['parsed_constraints'])
                        decoded = checkpoint['interpretation']
                        await emit({'type':'checkpoint_restored','node_id':'interpretation'})
                    else:
                        parsed,decoded = await interpret(self.settings,message,previous,parsed,constraints,emit,self.model_transport,ctx.business_timezone)
                    if self.store and self.task_id:
                        from orchestration.state import TaskResult
                        await self.store.checkpoint(self.task_id,TaskNode(id='interpretation',agent='supervisor',operation='interpret',volatile=False),
                            TaskResult(node_id='interpretation',agent='supervisor',status='completed',data={'fingerprint':fingerprint,'interpretation':decoded,'parsed_constraints':parsed.model_dump()}))
                except (AgentError,ValueError):
                    from orchestration.dialogue import exclusive_shop_keyword
                    if exclusive_shop_keyword(message):
                        raise AgentError('CONSTRAINT_PARSE_FAILED','无法可靠保留指定店铺条件，请明确店名后重试',502)
                    await emit({'type':'degradation','code':'INTERPRETER_UNAVAILABLE','message':'模型解析不可用，保留明确的结构化条件'})
            missing = self.planner.missing(parsed)
            if parsed.visit_time:
                from datetime import datetime
                from zoneinfo import ZoneInfo
                from decision.ranking import parse_time
                if parse_time(parsed.visit_time,ZoneInfo(ctx.business_timezone)) < datetime.now(ZoneInfo(ctx.business_timezone)):
                    parsed.visit_time=None
                    if 'visit_time' not in missing: missing.append('visit_time')
            if missing:
                labels={'location':'当前位置经纬度','people':'用餐人数','budget_per_person':'人均预算','visit_time':'具体用餐日期和时刻'}
                facts={'known':'已保留用户条件：'+json.dumps(parsed.model_dump(),ensure_ascii=False),
                       'missing':'还缺少：'+'、'.join(labels[key] for key in missing)+'。请用户补充，不能自行猜测。'}
                authored={'answer':'已保留你的条件。请补充：'+'；'.join(labels[key] for key in missing)+'。','answer_origin':'deterministic'}
                if self.settings.configured:
                    authored=await compose(self.settings,message,facts,emit,transport=self.model_transport,purpose='clarification_answer')
                return {**authored,'state':AgentState.WAITING_INPUT,'intent':'recommend','constraints':parsed.model_dump(),'missing_fields':missing}
            return await self.recommend(parsed,ctx,emit,explanation,message=message)
        if intent.kind == "knowledge" and ctx.rag:
            return await ctx.rag.answer(message, emit)
        if intent.kind == "action":
            if self.store:
                import re
                from api.actions import ActionRequest, ActionService
                operation = "cancel" if "取消" in message else "mock_pay" if "模拟支付" in message else "seckill"
                target = re.search(r'(?:优惠券|券)\s*([0-9]{1,19})|(?:抢|购买|下单)\s*([0-9]{1,19})\s*(?:号)?(?:优惠)?券', message) if operation == 'seckill' else re.search(r'订单\s*([0-9]{1,19})|(?:取消|模拟支付)\s*([0-9]{1,19})\s*(?:号)?订单', message)
                if target:
                    target_id = target[1] or target[2]
                    values = {"operation": operation, "voucher_id" if operation == "seckill" else "order_id": int(target_id) if operation == "seckill" else target_id}
                    # A recovered execution of the same task must reuse its action.
                    # Epoch/worker identity must never become a new business key.
                    if self.task_id:
                        from uuid import UUID, uuid5
                        values['idempotency_key'] = str(uuid5(UUID(self.task_id), operation + ':' + str(target_id)))
                    action = await ActionService(self.store, self.settings).prepare(ActionRequest.model_validate(values), ctx, task_id=self.task_id)
                    return {"answer": "请核对下方操作摘要，只有点击确认后才会提交。" + action["preview"]["notice"],
                            "intent": "action", "state": AgentState.WAITING_CONFIRMATION, "action": action}
            return {"answer": "交易需要登录并核对操作摘要，再由本人显式确认；请使用交易确认入口。支付目前仅有开发模拟通道。",
                    "intent": "action", "state": AgentState.WAITING_CONFIRMATION}
        if not self.settings.configured:
            raise AgentError("MODEL_NOT_CONFIGURED", "请配置模型后使用开放式业务问答", 503)
        async with open_model(self.settings, self.model_transport) as client:
            answer = await AgentEngine(client, self.settings.model, self.settings.max_rounds, reasoning_effort=self.settings.reasoning_effort).run(message, history, ctx, emit)
        return {"answer": answer, "state": AgentState.COMPLETED, "intent": "legacy"}

    async def recommend(self, constraints, ctx, emit, explanation, message=None):
        graph = TaskGraph()
        async def checkpoint(node, result):
            if self.store and self.task_id:
                await self.store.checkpoint(self.task_id, node, result)
        async def shops_handler(_node, _results):
            return await self.shop.candidates(constraints, ctx, emit)
        plan = TaskPlan(nodes=[TaskNode(id="shops", agent="shop", operation="candidates", timeout=45)])
        await emit({"type": "plan", "nodes": [node.model_dump() for node in plan.nodes]})
        found = (await graph.run(plan, shops_handler, emit=emit, checkpoint=checkpoint))["shops"]
        if found.status != "completed":
            raise AgentError(found.error.code, found.error.message)
        shops = found.data["shops"]
        offers_plan = self.planner.offers(shops)
        await emit({"type": "plan", "nodes": [node.model_dump() for node in offers_plan.nodes]})
        async def offers_handler(node, _results):
            if node.id == "ranking":
                offers, errors = {}, list(found.data.get("warnings", []))
                for shop in shops:
                    result = _results[f"voucher_{shop['id']}"]
                    if result.status == "completed":
                        offers[shop["id"]] = result.data["vouchers"]
                    else:
                        errors.append({"shop_id": shop["id"], **result.error.model_dump()})
                decision = DecisionEngine().rank(shops, offers, constraints, ctx.business_timezone)
                decision.update(warnings=errors, coverage=found.data["coverage"], truncated=found.data["truncated"])
                return decision
            return await self.voucher.for_shop(node.arguments["shop_id"], ctx, emit)
        offers_results = await graph.run(offers_plan, offers_handler, emit=emit, checkpoint=checkpoint, restored={"shops": found})
        ranked = offers_results["ranking"]
        if ranked.status != "completed":
            raise AgentError(ranked.error.code, ranked.error.message)
        decision = ranked.data
        await emit({"type": "decision", "result": decision})
        authored={'answer':self.format_decision(decision,explanation),'answer_origin':'deterministic'}
        if self.settings.configured:
            try:
                authored=await compose(self.settings,message or '解释消费决策',decision_facts(decision),emit,
                                       transport=self.model_transport,purpose='decision_explanation')
            except (AgentError,ValueError):
                authored['answer_origin']='deterministic_fallback'
                await emit({'type':'degradation','code':'EXPLANATION_UNAVAILABLE','message':'真实模型解释未通过，保留程序事实'})
        return {**authored,'intent':'recommend','state':AgentState.COMPLETED,'constraints':constraints.model_dump(),
                'decision':decision,'decision_context':decision_context(decision)}

    @staticmethod
    def format_decision(decision, explanation):
        parts = [explanation]
        for key, title in [("recommendations", "按明确规则通过校验的推荐"), ("unverified_candidates", "使用条件待确认的候选（不保证今晚可用）")]:
            rows = decision[key]
            if not rows:
                continue
            parts.append("\n### " + title + "\n\n| 店铺 | 距离 | 人均参考 | 券价/面值 | 券面节省 | 库存 |\n|---|---|---|---|---|---|")
            explanations = []
            for row in rows:
                name = str(row['shop_name']).replace('|', '｜').replace('\n', ' ')
                parts.append(f"| [{name}](/shops/{row['shop_id']}) | {row['distance_m']:.0f}米 | ¥{row['average_price']:.2f} | ¥{row['voucher_price']:.2f}/¥{row['face_value']:.2f} | ¥{row['face_saving']:.2f} | {row['stock']}张 |")
                explanations.append(f"\n券{row['voucher_id']}：{row['eligibility_reason']}。按{row['people']}人和店铺参考人均估算，使用一张券后合计约¥{row['estimated_cash_cost']:.2f}，人均约¥{row['estimated_per_person']:.2f}。\n")
            parts.extend(explanations)
        if not decision["recommendations"]:
            parts.append("\n没有足够证据确认满足全部条件的优惠，不能凑出三家已验证推荐。")
        if not decision["recommendations"] and not decision["unverified_candidates"]:
            parts.append("当前查询范围内没有符合条件且有实时库存的可评估秒杀券。可以调整预算、半径或时间。")
        if decision["warnings"]:
            parts.append("部分接口查询失败，以上仅包含查询成功的数据，并非全量比较。")
        parts.append("\n" + "；".join(decision["limitations"]) + "。" )
        return "\n".join(parts)

    @staticmethod
    def describe(tool, data):
        if tool == "order_status":
            return f"订单{data.get('orderId')}：{data.get('statusText', '状态未知')}（状态码{data.get('statusCode')}）。"
        if tool == "shop_details":
            return f"{data.get('name', '店铺')}（ID {data.get('id')}）\n地址：{data.get('address', '未提供')}\n营业时间：{data.get('openHours', '未提供')}\n人均参考：{data.get('avgPrice', '未知')}元。"
        if tool == "my_vouchers":
            records = data.get("records", [])
            return "个人券包：\n" + ("\n".join(f"- 订单{r.get('orderId')} · {r.get('title', '优惠券')} · {r.get('statusText', '状态未知')}" for r in records) or "当前页没有记录。")
        return "接口查询结果：\n\n```json\n" + json.dumps(data, ensure_ascii=False, indent=2, default=str)[:18000] + "\n```"
