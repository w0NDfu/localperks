"""Evidence sufficiency is distinct from semantic relevance. Fail closed on facts absent from KB."""
import re


CATALOG = [
    ('transaction',r'确认|幂等|网络|重试|结果未知',r'显式确认|网络超时|幂等键',
     '交易需要本人核对并显式确认，未知结果应只读核对，不应直接重复写操作。'),
    ('estimate',r'人均|参考价|估算|账单|公式|实付',r'菜单报价|估算实付|参考值',
     '人均参考价不是菜单报价；估算仅按一张券计算，不能保证实际结账金额。'),
    ('ownership',r'别人|他人|归属|登录|隐私|本人|别的用户',r'订单归属|当前登录用户.*自己|模型不能指定他人|只能访问本人',
     '个人订单和券包必须登录且核验本人归属，模型不能指定其他用户的身份。'),
    ('cancel',r'取消|未付款|未支付|已付.*款|超时订单',r'未支付.*取消|取消.*未支付',
     '只允许取消本人的未支付订单；已支付订单不能按取消未支付订单处理，重复取消不重复回补。'),
    ('coupon',r'有效期|核销|开抢|使用规则|购买活动|买了券|商家说|叠加|人数|套餐|今晚',r'不代表.*核销|使用日期|使用规则|购买时间不等于使用日期',
     '购买活动时间不代表核销有效期；使用日期、人数及叠加限制要看明确规则，规则不清不能保证可用。'),
    ('payment',r'退款|售后|支付|扣款',r'没有真实支付通道|真实退款.*尚未实现|退款.*API.*未实现',
     '项目只有开发模拟支付，没有真实支付通道；未实现的退款或售后能力不能承诺已执行。'),
    ('facilities',r'座位|包间|停车|预约|设施',r'座位.*没有|座位.*未|接待能力|不能承诺未提供',
     '当前业务字段不能证明座位、包间、预约或停车条件，需向商家另行核实。'),
    ('order',r'排队|处理中|提交|收到|预扣|异步|状态码|秒杀.*原理',r'只代表请求已接收|10排队中|排队.*处理中',
     '提交请求或Lua预扣不等于最终成单；应继续查询异步订单状态，排队和处理中不能当作成功。'),
]


def assess(query,hits):
    if not hits:return {'sufficient':False,'reason':'没有检索到能够回答该问题的证据','claims':[]}
    specific = (re.search(r'(?:店铺|餐厅|商家|券|优惠券)\s*\d+|这家|那家|当前|今天|今晚|现在',query)
                and re.search(r'订金|多少|价格|库存|剩余|几人|几个人|能用|能使用|确定.*用',query))
    if specific and not re.search(r'知识库|文档|为什么|规则不|规则没有|没有.*规则',query):
        return {'sufficient':False,'reason':'知识文档没有该商家或券的实时金额、库存、接待能力或具体日期资格，须查实时业务或向商家确认','claims':[]}
    out_of_scope=re.search(r'到账|限行|天气|航班|地铁|量子|世界杯|订金.*(?:元|金额)|具体.*(?:订金|菜单价)',query)
    bank_fact='银行卡' in query and not re.search(r'项目|系统|支付说明|模拟支付',query)
    if out_of_scope or bank_fact:
        return {'sufficient':False,'reason':'检索结果即使相关，也没有该具体事实的可核验值，不能据此生成答案','claims':[]}
    claims=[]
    for key,trigger,proof,claim in CATALOG:
        if re.search(trigger,query):
            sources=[hit for hit in hits if re.search(proof,hit['text'])]
            if sources:claims.append({'id':key,'claim':claim,'source_ids':[s['id'] for s in sources]})
    if not claims:return {'sufficient':False,'reason':'相关文档没有覆盖问题要求的业务命题，不能仅凭相似度作答','claims':[]}
    return {'sufficient':True,'reason':'回答范围限于源文档能够证明的业务命题','claims':claims}
