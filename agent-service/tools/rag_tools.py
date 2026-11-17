from typing import Annotated
from pydantic import Field
from core.context import ToolContext
from tools.registry import registry


async def knowledge(ctx, query):
    if ctx.rag is None:
        return {'error': {'code': 'RAG_NOT_CONFIGURED', 'message': '知识服务尚未配置，不能确认文档规则'}}
    try:
        result = await ctx.rag.search(query)
    except Exception:
        return {'error': {'code': 'RAG_UNAVAILABLE', 'message': '知识服务暂不可用，请查询券详情或联系商家'}}
    return {'evidence': [{key: hit[key] for key in ('id', 'text', 'source', 'title')} for hit in result['hits']],
            'warning': '文档不能证明实时价格、库存或个人订单状态', 'strategy': result['strategy']}


@registry.tool(agent='shop', description='按需检索店铺服务说明和平台文档，返回引用证据。不用于实时价格、库存。')
async def shop_knowledge(ctx: ToolContext, query: Annotated[str, Field(min_length=2, max_length=500)]):
    return await knowledge(ctx, query)


@registry.tool(agent='voucher', description='按需检索优惠券使用规则和秒杀说明，返回引用证据；当前价格和库存必须另查业务API。')
async def voucher_knowledge(ctx: ToolContext, query: Annotated[str, Field(min_length=2, max_length=500)]):
    return await knowledge(ctx, query)


@registry.tool(agent='order', description='按需检索订单、支付和售后FAQ；个人订单状态必须另查JavaAPI。')
async def order_knowledge(ctx: ToolContext, query: Annotated[str, Field(min_length=2, max_length=500)]):
    return await knowledge(ctx, query)
