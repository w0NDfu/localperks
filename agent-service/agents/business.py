import asyncio
import re

from orchestration.state import ConsumptionConstraints, TaskResult


class BusinessAgent:
    def __init__(self, name, executor):
        self.name, self.executor = name, executor

    async def call(self, tool, arguments, ctx, emit=None) -> TaskResult:
        return await self.executor.execute(self.name, tool, arguments, ctx, emit)


class ShopAgent(BusinessAgent):
    def __init__(self, executor):
        super().__init__("shop", executor)

    async def candidates(self, constraints: ConsumptionConstraints, ctx, emit=None):
        categories = await self.call("shop_types", {}, ctx, emit)
        if categories.status != "completed":
            return categories
        types = categories.data
        if not isinstance(types, list):
            raise ValueError("Invalid category response")
        food_ids = {row["id"] for row in types if re.search(r"美食|餐|料理|火锅|烧烤|小吃|日料|\bfood\b|\brestaurants?\b|\bdining\b", row.get("name", ""), re.I)}
        if not food_ids and constraints.meal_only:
            raise ValueError("没有可确认的餐饮分类")
        selected = sorted(food_ids if constraints.meal_only else {row["id"] for row in types})[:12]
        limit = asyncio.Semaphore(3)

        async def fetch(type_id):
            found, truncated = [], False
            async with limit:
                for page in range(1, 5):
                    args = {"type_id": type_id, "page": page}
                    if constraints.location:
                        args.update(constraints.location.model_dump())
                    result = await self.call("shops_by_type", args, ctx, emit)
                    if result.status != "completed":
                        return found, True, result.error.model_dump()
                    found.extend(result.data["shops"])
                    category=next((row.get('name','') for row in types if row['id']==type_id),'')
                    for shop in result.data['shops']:
                        shop['category_name']=category
                    if not result.data["has_more"]:
                        break
                else:
                    truncated = True
            return found, truncated, None

        batches = await asyncio.gather(*(fetch(type_id) for type_id in selected))
        shops = {row["id"]: row for batch, _, _ in batches for row in batch}
        if constraints.keyword:
            shops = {key: row for key, row in shops.items() if constraints.keyword.casefold() in row.get("name", "").casefold()}
        errors = [error for _, _, error in batches if error]
        if errors and not shops:
            return TaskResult(node_id="shops", agent="shop", status="failed", error=errors[0])
        return {"shops": list(shops.values())[:40], "coverage": "所查分类、半径内最多四页/分类，最多40家",
                "truncated": any(flag for _, flag, _ in batches) or len(shops) > 40,
                "warnings": errors}


class VoucherAgent(BusinessAgent):
    def __init__(self, executor):
        super().__init__("voucher", executor)

    async def for_shop(self, shop_id, ctx, emit=None):
        return await self.call("shop_vouchers", {"shop_id": shop_id}, ctx, emit)


class OrderAgent(BusinessAgent):
    def __init__(self, executor):
        super().__init__("order", executor)

    async def status(self, order_id, ctx, emit=None):
        return await self.call("order_status", {"order_id": order_id}, ctx, emit)

    async def track(self, order_id, ctx, emit, seconds=20, interval=1):
        import time
        from core.state import AgentError
        until, last = time.monotonic() + seconds, None
        labels = {10: "排队中", 11: "处理中", 12: "待支付", 13: "成功", 14: "取消", 15: "失败",
                  16: "已核销", 17: "退款中", 18: "已退款"}
        while True:
            result = await self.status(order_id, ctx, emit)
            if result.status != "completed":
                raise AgentError(result.error.code, result.error.message)
            last = result.data
            await emit({"type": "order_status", "agent": "order", "order_id": order_id,
                        "status": labels.get(last.get("statusCode"), "未知"), "result": last})
            if last.get("statusCode") not in (10, 11) or time.monotonic() >= until:
                break
            await asyncio.sleep(min(interval, max(0, until - time.monotonic())))
        return last
