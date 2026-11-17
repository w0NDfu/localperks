from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Annotated

from pydantic import Field

from core.context import ToolContext
from tools.registry import registry
from tools.java_api import ShopLinkError


@registry.tool(agent="voucher", description="按店铺ID查询该店可售优惠券与实时秒杀库存，返回原始购买活动时间和使用规则。")
async def shop_vouchers(ctx: ToolContext, shop_id: Annotated[int, Field(ge=1)]):
    rows = await ctx.java.get(f"/voucher/list/{shop_id}", evidence=ctx.requests)
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ShopLinkError(502, "SHOPLINK_INVALID_RESPONSE", "优惠券接口返回格式无效")
    return {"vouchers": rows, "shop_id": shop_id,
            "queriedAt": datetime.now(ZoneInfo(ctx.business_timezone)).isoformat()}


@registry.tool(agent="voucher", description="查询全站优惠券和Redis实时库存。可按商家ID、关键词、最高售价（元）过滤。每次最多返回20条，并提示是否截断。")
async def find_vouchers(
    ctx: ToolContext,
    shop_id: Annotated[int | None, Field(ge=1)] = None,
    keyword: Annotated[str, Field(max_length=128)] = "",
    max_price: Annotated[float | None, Field(ge=0, le=100000)] = None,
    seckill_only: bool = False,
):
    rows = await ctx.java.get("/voucher/market", evidence=ctx.requests)
    matches = []
    zone = ZoneInfo(ctx.business_timezone)
    now = datetime.now(zone)
    for row in rows:
        if shop_id is not None and row.get("shopId") != shop_id:
            continue
        if seckill_only and row.get("type") != 1:
            continue
        if keyword and keyword.casefold() not in (str(row.get("shopName", "")) + str(row.get("title", ""))).casefold():
            continue
        cents = row.get("type") == 1
        price = row.get("payValue")
        yuan = price / 100 if cents and price is not None else price
        if max_price is not None and (yuan is None or yuan > max_price):
            continue
        item = dict(row)
        item["priceYuan"] = yuan
        amount = item.get("actualValue")
        item["valueYuan"] = amount / 100 if cents and amount is not None else amount
        item["activityState"] = "普通券"
        if cents:
            try:
                begin = datetime.fromisoformat(item["beginTime"])
                end = datetime.fromisoformat(item["endTime"])
                begin = begin.replace(tzinfo=zone) if begin.tzinfo is None else begin.astimezone(zone)
                end = end.replace(tzinfo=zone) if end.tzinfo is None else end.astimezone(zone)
                if now < begin:
                    item["activityState"] = "未开始"
                elif now >= end:
                    item["activityState"] = "已结束"
                elif item.get("stock") is None:
                    item["activityState"] = "库存未知"
                elif item["stock"] <= 0:
                    item["activityState"] = "已抢完"
                else:
                    item["activityState"] = "可抢"
            except (KeyError, TypeError, ValueError):
                item["activityState"] = "活动时间未知"
        matches.append(item)
    return {"vouchers": matches[:20], "matched_count": len(matches), "truncated": len(matches) > 20,
            "queriedAt": now.isoformat(timespec="seconds"), "timezone": ctx.business_timezone}

