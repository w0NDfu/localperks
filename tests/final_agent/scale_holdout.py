"""Generate preregistered synthetic holdouts. Each case owns separate facts or a distinct question."""
RAG = [
    ('状态码12没有支付时间，与状态码13有何业务差别？客服能否认为都已经获得券？', ['订单状态与异步查询']),
    ('客服看到状态码14与17，请用文档说明取消和退款中的区别，以及能否用同一操作推进。', ['订单状态与异步查询', '取消订单与超时', '支付与售后']),
    ('同一券再次提交返回原单号，先按秒杀规则和异步状态解释该继续确认哪一笔订单。', ['秒杀购买与使用规则', '订单状态与异步查询']),
    ('按消费估算说明，当人均参考价乘人数低于券面值时，估算实付里的两项分别如何处理？', ['推荐指标与消费估算']),
    ('历史展示状态码18，客服能否用它证明这次系统已经主动执行真实退款？', ['订单状态与异步查询', '支付与售后']),
    ('把营业时间用作筛选时，可以再据此承诺店里有双人座位并完成预约吗？', ['店铺服务说明']),
    ('店铺31250每日是否提供无障碍洗手间？给出当前可用结论。', []),
    ('哪家店从指定公历跨年日开始禁止宠物入内？请直接给店名。', []),
    ('跨区券失效后平台按什么比例补偿积分？我需要明确百分比。', []),
]

OPS_FAMILIES = [
    ('lower-stock', 'REDIS_STOCK_DRIFT', '销售人员怀疑库存不足。复盘在途和两侧余量，验证差异是否持续。'),
    ('higher-stock', 'REDIS_STOCK_DRIFT', '审核看到可抢数量偏高。请复盘账实关系和预留，说明可确认的时序。'),
    ('cache-missing', 'INSUFFICIENT_EVIDENCE', '库存数字突然没有返回。复盘观测缺口，评估是否能确认有库存漂移。'),
    ('historical', 'INSUFFICIENT_EVIDENCE', '复盘早先窗口内的结算异常，核对旧窗口与本次观测的时序。'),
    ('ip-limit', 'IP_RATE_LIMIT', '集中请求有拒绝反馈。结合业务时序，核实拒绝的准确维度。'),
    ('duplicate', 'DUPLICATE_DELIVERY', '同一交易在追踪中出现多次。请复盘投递、处理与数据库结果。'),
    ('mq-backlog', 'MQ_CONSUME_DISABLED', '有新请求受理但订单没进账。复盘投递与消费，核对在途预留。'),
]


def outcome(facts, people, budget, radius, hour, partial=False):
    verified, unknown = [], []
    for item in facts:
        if partial and item['index'] == 1:
            continue
        if (item['avg'] > budget or item['offset_m_approx'] > radius or hour >= item['close'] or
                not item['active'] or item['stock'] == 0 or item['max_party'] < people):
            continue
        cash = (max(item['avg'] * people - 100, 0) * 100 + item['pay']) / 100
        if cash > budget * people:
            continue
        row = {'shop_id': item['shop'], 'voucher_id': item['voucher'], 'estimated_cash_cost': cash, 'stock': item['stock']}
        (unknown if item['unknown'] else verified).append(row)
    return {'verified': verified, 'unknown': unknown, 'people': people, 'budget': budget, 'radius': radius,
            'partial': partial}


def decisions():
    tasks = []
    for index in range(56):
        category = 'decision' if index < 40 else 'multiturn' if index < 48 else 'anomaly'
        people = 2 + index % 4
        budget = 78 + (index * 13) % 41
        radius = 450 + index % 7 * 25
        hour = 18 + (index // 4) % 3
        lat, lon = round(30.20 + (index // 20) * .03, 5), round(120.10 + (index % 20) * .023, 5)
        facts = []
        for slot in range(3):
            item = {
                'index': slot, 'shop': 31000 + index * 3 + slot, 'voucher': 41000 + index * 3 + slot,
                'name': f'独立样本{index + 1:02d}餐厅{chr(65 + slot)}',
                'avg': budget - (12 + index % 6) + slot * 2, 'close': 23, 'pay': 2900 + (index * 137) % 700 + slot * 1700,
                'stock': 4 + (index * 7 + slot) % 19, 'active': True, 'max_party': 50, 'unknown': slot == 2,
                'offset_m_approx': 75 + slot * 85, 'x': round(lon + .0008 + slot * .0009, 5), 'y': lat,
            }
            flags = ((index * 29 + 9) >> slot) & 15
            if slot == 1:
                if flags & 1: item['avg'] = budget + 9 + index % 8
                if flags & 2: item['max_party'] = people - 1
                if flags & 4: item['stock'] = 0
                if flags & 8: item['close'] = hour
            if slot == 0 and index % 11 == 7: item['active'] = False
            if slot == 2 and index % 5 == 1:
                item.update(x=round(lon + .008, 5), offset_m_approx=770)
            if slot == 2 and index % 7 == 2: item['unknown'] = False
            if category == 'anomaly' and slot == 1:
                item.update(avg=budget - 7, stock=9 + index % 4, max_party=50, close=23)
            if item['unknown']:
                item['rules'] = '以店铺公告为准，另行核对'
            elif item['max_party'] != 50:
                item['rules'] = f'长期有效；全天可用；每桌限1张；{item["max_party"]}人可用'
            else:
                item['rules'] = '长期有效；全天可用；每桌限1张'
            facts.append(item)
        slots = (people, budget, radius, hour)
        turns = [{'slots': slots, 'expected': outcome(facts, *slots, partial=category == 'anomaly')}]
        if category == 'multiturn':
            second = (people + 1, budget - (4 + index % 4), radius, hour)
            turns.append({'slots': second, 'expected': outcome(facts, *second)})
        tasks.append({'id': f'{category}-{index + 1:02d}', 'group': category, 'facts': facts, 'turns': turns,
                      'lon': lon, 'lat': lat, 'fault_shop': facts[1]['shop'] if category == 'anomaly' else None})
    return tasks


def freeze():
    tasks = decisions()
    tasks.extend({'id': f'rag-{i:02d}', 'group': 'rag', 'message': msg, 'relevant_titles': titles}
                 for i, (msg, titles) in enumerate(RAG, 1))
    for index, (family, root, msg) in enumerate(OPS_FAMILIES):
        for replica in range(5):
            seq = index * 5 + replica
            tasks.append({'id': f'ops-{seq + 1:02d}', 'group': 'ops', 'family': family, 'root': root,
                          'stock': 30 + seq * 2, 'delta': 2 + replica * 2, 'events': 1 + replica % 3,
                          'voucher': 44001 + seq, 'replica': replica,
                          'symptom': f'请核查券{44001 + seq}的观测窗口。{msg}'})
    assert len(tasks) == len({task['id'] for task in tasks}) == 100
    return tasks
