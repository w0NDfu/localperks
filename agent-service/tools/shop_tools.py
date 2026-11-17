from typing import Annotated, Any

import httpx
from pydantic import Field

from core.context import ToolContext
from tools.java_api import JavaAPI, ShopLinkError
from tools.registry import registry

Page = Annotated[int, Field(ge=1, le=100)]
ShopId = Annotated[int, Field(ge=1)]


async def search_shops(client: httpx.AsyncClient, keyword: str, page: int) -> list[dict[str, Any]]:
    result = await JavaAPI(client).get("/shop/of/name", params={"name": keyword.strip(), "current": page})
    if not isinstance(result, list) or not all(isinstance(item, dict) for item in result):
        raise ShopLinkError(502, "SHOPLINK_INVALID_RESPONSE", "LocalPerks 返回了无效店铺列表")
    return result


@registry.tool(agent="shop", description="按店名关键词分页查询真实店铺。关键词留空可浏览店铺。每页最多10条。")
async def find_shops(ctx: ToolContext, keyword: Annotated[str, Field(max_length=128)] = "", page: Page = 1):
    items = await ctx.java.get("/shop/of/name", params={"name": keyword.strip(), "current": page},
                               evidence=ctx.requests)
    return {"shops": items, "page": page, "has_more": len(items) == 10}


@registry.tool(agent="shop", description="根据真实店铺ID查询详细资料，包括地址、人均、评分和营业时间。")
async def shop_details(ctx: ToolContext, shop_id: ShopId):
    return await ctx.java.get(f"/shop/{shop_id}", evidence=ctx.requests)


@registry.tool(agent="shop", description="查询店铺分类列表，获取分类ID。")
async def shop_types(ctx: ToolContext):
    return await ctx.java.get("/shop-type/list", evidence=ctx.requests)


@registry.tool(agent="shop", description="根据分类查询店铺。经纬度必须同时提供，提供后查询5公里内附近店铺；不提供时按分类分页。")
async def shops_by_type(
    ctx: ToolContext,
    type_id: Annotated[int, Field(ge=1)],
    page: Page = 1,
    longitude: Annotated[float | None, Field(ge=-180, le=180)] = None,
    latitude: Annotated[float | None, Field(ge=-85.05112878, le=85.05112878)] = None,
):
    if (longitude is None) != (latitude is None):
        return {"error": {"code": "LOCATION_REQUIRED", "message": "经度和纬度需同时提供"}}
    params = {"typeId": type_id, "current": page}
    if longitude is not None:
        params.update(x=longitude, y=latitude)
    items = await ctx.java.get("/shop/of/type", params=params, evidence=ctx.requests)
    return {"shops": items, "page": page, "has_more": len(items) == 5}

