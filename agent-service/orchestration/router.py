import re

from orchestration.state import Intent


class Router:
    """Only unambiguous forms bypass the LLM; uncertainty retains legacy ReAct."""

    def route(self, text: str, previous: dict | None = None) -> Intent:
        text = text.strip()
        match = re.fullmatch(r"(?:持续)?(?:跟踪|追踪)订单\s*([0-9]{1,19})(?:的)?(?:状态|进度)?[？?。！!]?", text)
        if match:
            return Intent(kind="simple", agent="order", tool="track_order", arguments={"order_id": match[1]})
        if re.search(r"(帮我|我要|请)?(抢|购买|下单).*券|取消.*订单|模拟支付", text) and not re.search(r"规则|如何|怎么|说明|政策|记录|券包", text):
            return Intent(kind="action")
        if re.search(r"推荐|最划算", text) and re.search(r"附近|人均|预算|吃饭|餐厅|店", text):
            return Intent(kind="recommend")
        if re.search(r"规则|怎么使用|如何使用|怎么抢|如何抢|能否叠加|能叠加|退款政策|支付说明|售后|为什么.*(取消|失败)|秒杀.*原理", text) and not re.search(r"实时库存|当前库存|现在.*(库存|价格)|当前订单状态", text):
            return Intent(kind="knowledge")
        if re.search(r"推荐|最划算|两个人.*吃饭|人均.*(附近|优惠)|附近.*(人均|优惠)", text):
            return Intent(kind="recommend")
        if previous and re.search(r"改成|改为|预算|人均|两人|三人|四人|今晚|明晚|经纬度|公里|不吃|不要推荐|排除|偏好|喜欢|简洁|简短|详细|这次想吃|今天想吃|这次就吃|今天就吃|可以吃|不限分类|\d+\.\d+\s*[,，]\s*\d+\.\d+", text):
            return Intent(kind="recommend")
        if re.fullmatch(r"(?:帮我|查询|查看|查一下|看看|查)?\s*我的(?:券包|优惠券|券|抢券记录)[？?。！!]?", text):
            return Intent(kind="simple", agent="order", tool="my_vouchers")
        match = re.fullmatch(r"(?:查询|查看|帮我查|查)?\s*订单\s*([0-9]{1,19})\s*(?:的)?(?:状态|进度)[？?。！!]?", text)
        if match:
            return Intent(kind="simple", agent="order", tool="order_status", arguments={"order_id": match[1]})
        match = re.fullmatch(r"(?:查询|查看|查)?\s*(?:店铺|商家)\s*(\d+)\s*(?:的)?(?:详情|地址|营业时间)[？?。！!]?", text)
        if match:
            return Intent(kind="simple", agent="shop", tool="shop_details", arguments={"shop_id": int(match[1])})
        match = re.fullmatch(r"(?:查询|查看|查)?\s*(?:店铺|商家)\s*(\d+)\s*(?:的|有)?(?:优惠券|秒杀券)[？?。！!]?", text)
        if match:
            return Intent(kind="simple", agent="voucher", tool="find_vouchers", arguments={"shop_id": int(match[1]), "seckill_only": "秒杀" in text})
        match = re.fullmatch(r"(?:查询|查看|查|找)\s*[「“\"]?([^，,。？?]{1,40}?)[」”\"]?\s*(?:店铺|门店)[？?。！!]?", text)
        if match and not re.search(r"再|然后|优惠|附近", match[1]):
            return Intent(kind="simple", agent="shop", tool="find_shops", arguments={"keyword": match[1]})
        if re.fullmatch(r"(?:查询|查看|查)?(?:店铺)?分类(?:列表)?[？?。！!]?", text):
            return Intent(kind="simple", agent="shop", tool="shop_types")
        if re.fullmatch(r"(?:查询|查看|看看|有哪些|有什么)?(?:全部|当前|全站)?(?:优惠券|秒杀券)(?:列表)?[？?。！!]?", text):
            return Intent(kind="simple", agent="voucher", tool="find_vouchers", arguments={"seckill_only": "秒杀" in text})
        match = re.fullmatch(r"(?:帮我)?(?:找|查|查询)\s*([^，,。？?]{1,30})店[？?。！!]?", text)
        if match and not re.search(r"附近|再|然后|优惠|推荐", text):
            return Intent(kind="simple", agent="shop", tool="find_shops", arguments={"keyword": match[1]})
        return Intent(kind="legacy")
