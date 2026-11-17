from typing import Annotated

from pydantic import Field

from core.context import ToolContext
from tools.registry import registry


@registry.tool(agent="order", risk="personal", description="查询当前登录用户的券包和抢券记录。每页12条，owned_only=true仅查询已获得的券。")
async def my_vouchers(ctx: ToolContext, page: Annotated[int, Field(ge=1, le=100)] = 1, owned_only: bool = False):
    result = await ctx.java.get("/voucher-order/mine", params={"page": page, "ownedOnly": str(owned_only).lower()},
                              authenticated=True, evidence=ctx.requests)
    if isinstance(result, dict):
        for row in result.get("records", []):
            for raw, normalized in (("payValue", "priceYuan"), ("actualValue", "valueYuan")):
                value = row.get(raw)
                row[normalized] = value / 100 if isinstance(value, (int, float)) else None
    return result


@registry.tool(agent="order", risk="personal", description="用完整订单号查询当前用户自己的订单状态。订单号必须保持十进制字符串。")
async def order_status(ctx: ToolContext, order_id: Annotated[str, Field(pattern=r"^[0-9]{1,19}$")]):
    if int(order_id) > 9_223_372_036_854_775_807:
        return {"error": {"code": "INVALID_ORDER_ID", "message": "订单号超过支持范围"}}
    return await ctx.java.get(f"/voucher-order/status/{order_id}", authenticated=True, evidence=ctx.requests)


