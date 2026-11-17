"""Deterministic window correlation and business checks. Never chooses a model's root."""
from collections import Counter

ROOTS = {
 'IP_RATE_LIMIT':'请求命中IP滑动窗口限流',
 'MQ_CONSUME_DISABLED':'消费组关闭消费导致消息积压',
 'REDIS_STOCK_DRIFT':'Redis与MySQL库存存在无法用在途预扣解释的持续差额',
 'DUPLICATE_DELIVERY':'同一订单消息重复投递，幂等处理拦截重复订单',
 'NO_FAULT':'在当前可观测范围没有证据证明这四类故障',
 'INSUFFICIENT_EVIDENCE':'证据不完整，不能确认根因',
}
ADVICE = {
 'IP_RATE_LIMIT':'核对共享出口、可信代理与实际流量；确认业务需求后由管理员评审限流配置，避免直接关闭保护。',
 'MQ_CONSUME_DISABLED':'由管理员核对消费组开关和变更记录，评估后恢复消费并观察位点追平；不要清空消息。',
 'REDIS_STOCK_DRIFT':'保留现场，核查库存写入及补偿记录，复核在途订单后制定人工对账方案；不要直接覆盖库存。',
 'DUPLICATE_DELIVERY':'检查生产重试和消费确认链路，持续核验幂等键、唯一约束和库存；当前证据不代表已产生重复订单。',
 'NO_FAULT':'继续观察；结论限于指定窗口、券及现有工具覆盖，不代表所有系统都健康。',
 'INSUFFICIENT_EVIDENCE':'补充同窗口日志、位点和业务快照；不可将查询失败或日志缺失解释为零故障。',
}


def analyze(observations):
    by={}; errors=[]
    for item in observations:
        if item.get('error'):errors.append(item['id']);continue
        by.setdefault(item['tool'],[]).append(item)
    claims={}; exclusions={}; metrics={}
    def support(root,ids,detail):claims[root]={'evidence_ids':list(dict.fromkeys(ids)),'detail':detail}
    traffic=by.get('traffic',[])
    if traffic:
        t=traffic[-1];d=t['raw']['data'];events=d.get('events',[])
        responses=[e for e in events if e['type']=='http' and e.get('status')==429]
        blocked=[e for e in responses if e.get('limitScope')=='ip']
        metrics['http429']=len(responses);metrics['ip_rejected']=len(blocked)
        if blocked and responses:support('IP_RATE_LIMIT',[t['id']],f"窗口内IP拒绝{len(blocked)}次且HTTP429共{len(responses)}次；事件记录了实际命中的限流维度。")
        elif d.get('coverageComplete'):exclusions['IP_RATE_LIMIT']='该窗口未观察到同时成立的IP拒绝和HTTP429证据'
    mq=by.get('mq',[])
    if mq:
        m=mq[-1];d=m['raw']['data'];metrics['lag']=d['lag']
        if d['lag']>0 and d['consumeEnabled'] is False:
            support('MQ_CONSUME_DISABLED',[m['id']],f"Broker位点{d['brokerOffset']}、Consumer位点{d['consumerOffset']}、积压{d['lag']}；消费组consumeEnabled=false。")
        elif d['consumeEnabled']:exclusions['MQ_CONSUME_DISABLED']='消费组开关为开启；单次积压不能证明持续处理故障'
    inventories=by.get('inventory',[])
    if inventories:
        stable=[]
        for item in inventories:
            d=item['raw']['data'];mysql=d.get('mysql',[]);r=d.get('redisStock')
            if not mysql or r is None:continue
            pending=int(d['pending'][0]['total']);delta=int(mysql[0]['stock'])-int(r)
            stable.append((item,delta,pending,int(mysql[0]['stock']),int(r)))
        if stable:
            last,delta,pending,db,rs=stable[-1];metrics.update(stock_delta=delta,pending_reservations=pending)
            if delta==pending and delta>=0:exclusions['REDIS_STOCK_DRIFT']='库存差额与在途预扣一致，不能直接称库存漂移'
            elif len(stable)>=2:
                prev=stable[-2]
                separated=last['raw']['observedAt']-prev[0]['raw']['observedAt']>=300
                if separated and (delta,pending,db,rs)==prev[1:] and pending==0:
                    support('REDIS_STOCK_DRIFT',[prev[0]['id'],last['id']],f"两个相隔至少300毫秒的快照中MySQL={db}、Redis={rs}、差额={delta}、在途预留=0；直接写入者和最初成因尚无证据。")
    delivery=by.get('delivery',[])
    if delivery:
        x=delivery[-1];d=x['raw']['data'];events=d.get('events',[])
        received=Counter(e['orderId'] for e in events if e['type']=='mq_received')
        returned=Counter(e['orderId'] for e in events if e['type']=='mq_handler_returned')
        dedup=Counter(e['orderId'] for e in events if e['type']=='order_idempotent')
        orders=Counter(str(o['orderId']) for o in d.get('orders',[]))
        repeated=[order for order,count in received.items() if count>1 and returned[order]>1 and dedup[order]>0 and orders[order]==1]
        metrics['repeated_order_ids']=len(repeated)
        if repeated:support('DUPLICATE_DELIVERY',[x['id']],f"观察到{len(repeated)}个订单ID重复收到并返回、执行幂等分支，数据库每ID只有1行。处理返回不单独证明Broker已提交确认。")
        elif d.get('coverageComplete'):exclusions['DUPLICATE_DELIVERY']='窗口内没有同一订单重复投递且命中幂等分支的完整证据'
    complete=all(t in by for t in ('traffic','mq','inventory','delivery','compensation'))
    coverage=all(v[-1]['raw']['data'].get('coverageComplete',True) for k,v in by.items() if k in ('events','traffic','delivery'))
    if complete and coverage and not errors and not claims:
        inv_ok='REDIS_STOCK_DRIFT' in exclusions
        if inv_ok and metrics.get('lag')==0:
            support('NO_FAULT',[v[-1]['id'] for k,v in by.items() if k in ('traffic','mq','inventory','delivery','compensation')],
                    '五类查询成功，日志覆盖完整，库存差额可解释、积压为零，未观察到指定四类异常。')
    # Incomplete evidence is never equivalent to a healthy observation.
    if not claims:
        support('INSUFFICIENT_EVIDENCE',[x['id'] for x in observations], '调查覆盖不足或工具失败；需要补充证据。')
    return {'supported':claims,'excluded':exclusions,'metrics':metrics,'tools_seen':list(by),'errors':errors,'complete':complete,'coverage':coverage}
