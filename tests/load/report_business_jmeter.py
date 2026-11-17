"""Render the audited local mixed-business JMeter evidence; never synthesize results."""
import argparse
import csv
import json
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

ROOT = Path(__file__).resolve().parents[2]


def read(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def table(headers, rows):
    return '\n'.join(['| '+' | '.join(headers)+' |', '| '+' | '.join(['---']*len(headers))+' |']+
                     ['| '+' | '.join(map(str,row))+' |' for row in rows])


def report(run, prior=None):
    result=read(run/'business-results.json')
    audit=read(run/'reconciliation.json')
    final=read(run/'final_state.json')
    cleanup=read(run/'cleanup_report.json')
    if result.get('audit_pass') is not True or audit.get('pass') is not True:
        raise ValueError('A full, passing business reconciliation is required for a measured report')
    if any(row.get('status')=='retained' for row in cleanup['actions']):
        raise ValueError('Resource cleanup is incomplete')
    phases=result['phases']
    phase_rows=[]
    route_rows=[]
    for p in phases:
        phase_rows.append([p['name'],p['configured_seconds'],p['started_sessions'],p['http_samples'],
            f"{p['completed_response_rps_including_drain']:.2f}",
            '/'.join(str(p['latency_ms'][k]) for k in ('p50','p95','p99')),
            p['rate_limited'],p['sold_out'],p['unexpected_count'],p['new_order_receipts'],
            p['submit_to_visible_ms']['p99']])
        for label,r in p['routes'].items():
            route_rows.append([p['name'],label,r['count'],r['p50_ms'],r['p95_ms'],r['p99_ms'],r['max_ms']])
    normal=next(p for p in phases if p['name']=='normal')
    peak=next(p for p in phases if p['name']=='peak')
    cache_rows=(run/'cache-counters.jsonl').read_text(encoding='utf-8').splitlines()
    if not cache_rows:
        raise ValueError('Missing cache observations')
    cache=json.loads(cache_rows[-1])
    route=normal['routes']['seckill.submit.accepted']
    resume=(f"使用JMeter构建商家浏览、查券、抢券、查单、支付与取消的本地混合业务负载，"
        f"在2万家店铺、10万条历史订单背景下持续发压5分钟，完成{normal['http_samples']:,}次业务HTTP请求，"
        f"观测吞吐约{normal['completed_response_rps_including_drain']:.0f}请求/秒、HTTP P99为{normal['latency_ms']['p99']} ms；"
        f"抢券受理P99为{route['p99_ms']} ms，最终核对订单、库存及消息消费一致。")
    evidence='../artifacts/test-runs/'+run.name+'/'
    previous=''
    if prior:
        old_normal=read(prior/'normal-summary.json')
        old_peak=read(prior/'peak-summary.json')
        old_failure=read(prior/'business-failure.json')
        previous=f'''\n## 保留的首轮结果\n\n首轮运行`{prior.name}`的常态阶段完成{old_normal['http_samples']:,}次业务HTTP请求，P99为{old_normal['latency_ms']['p99']} ms，非预期异常{old_normal['unexpected_count']}次。峰值阶段完成{old_peak['http_samples']:,}次业务HTTP请求，非预期异常{old_peak['unexpected_count']}次、429拒绝{old_peak['rate_limited']}次，不能视作零异常通过。\n\n首轮运行器在峰值异常后提前清理，未完成全程对账，失败原因为`{old_failure['error']}`。本轮保留同样业务逻辑、数据规模、流量和限流，调整运行器为保留HTTP失败后继续对账，并补充原始连接错误信息；没有通过放宽限流、延长HTTP超时或删掉失败样本获取结果。\n\n[首轮常态汇总](../artifacts/test-runs/{prior.name}/normal-summary.json) · [首轮峰值汇总](../artifacts/test-runs/{prior.name}/peak-summary.json) · [首轮实时容器与进程核对](../artifacts/test-runs/{prior.name}/live-verification.json)。首轮样本不拼接到本轮分位数和对账结果中。\n'''
        if (prior/'peak-startup-observation.json').exists():
            startup=read(prior/'peak-startup-observation.json')
            previous+=f"\n首轮原始时间戳显示，峰值首个100 ms内实际发起{startup['first_second_100ms_buckets'].get('0',0)}个搜索请求，首秒共{startup['first_8_second_search_starts'].get('0',0)}个，之后每秒约100个。78次连接失败集中于起步同一秒。因此这档包含客户端冷启动聚集形成的额外短突发，不能据此直接断言后端持续100会话/秒必然失败；根因还需同步连接/accept队列证据。[启动时序核对](../artifacts/test-runs/{prior.name}/peak-startup-observation.json)。\n"
    text=f'''# 本地JMeter混合业务实测

本轮运行：`{run.name}`。业务HTTP与MySQL、Redis、RocketMQ均实际执行，没有调用模型。流量比例和数据规模为明确设定的本地业务场景，不是线上日志回放；报告只描述该负载下观测到的表现，不推断生产容量或最大吞吐。

## 业务负载

- 数据背景：20,003家店铺（其中20,000家为扩展样本）、1,000张普通券、100,000条历史已支付订单、10,000个历史用户；各阶段另建9张秒杀券和独立登录用户。
- 会话分布：90%浏览，6%购买后支付，2%购买后主动取消，1%等待超时关闭，1%访问少库存券。少库存券售罄是预期业务结果，未计为成单成功。
- 店铺选择：80%访问200家热点店铺，20%访问19,800家长尾店铺；未预热全部店铺缓存。
- 一次浏览包含分类/名称搜索、店铺详情和商家优惠券查询。购买会话再执行抢券、250 ms间隔的状态轮询、支付或取消、券包查询及终态校验；一部分用户用同一身份重复提交，核对订单号不变。
- 另有一个运营线程每5秒更新一家热点店铺，触发真实缓存删除与MQ广播。运营请求单独计数，不混入用户业务吞吐。
- 保留应用原有限流：秒杀全局300次/分钟、IP 30次/分钟、用户5次/分钟；合成客户端IP仅供隔离回环测试进程识别。限流拒绝单独报告，未用调高限流获得吞吐数字。
- JMeter 5.6.3 CLI，Open Model Thread Group；常态30会话/秒持续300秒，峰值100会话/秒持续180秒。每阶段另留35秒停止新增后的收尾时间。

## 环境与口径

Windows本机运行JMeter与Java，HTTP直连回环端口；MySQL 8、Redis 7.2和RocketMQ 5.3.2使用本次独占的容器、网络及磁盘卷。Java堆上限512 MiB、未限制CPU配额，数据库业务/核验连接预算为6/4；MySQL、Redis、NameServer、Broker各1 CPU，内存上限分别768/128/384/768 MiB。MySQL binlog和redo采用同步提交配置，Redis AOF/everysec，Broker SYNC_FLUSH。单Broker不代表多副本容灾。支付步骤调用项目的本地支付状态接口，不涉及外部支付机构耗时。

业务SQL、Lua、事务和限流逻辑未修改；隔离编译副本只加入缓存计数器、JVM/连接池观测，采样开销保留在结果中。JMeter与服务共享本机资源，无跨机网络、TLS或网关成本。

HTTP耗时为JMeter请求样本耗时，不包含人为思考时间。订单可见耗时从抢券请求发起到首次HTTP查询看到订单，包含轮询间隔和重复提交步骤，不等于纯数据库提交耗时。

下表吞吐为该阶段全部用户HTTP完成数除以首个请求发起至最后一个用户响应结束的窗口；包括业务拒绝和状态轮询，不是秒杀成单TPS。阶段末无新增请求的35秒空闲时间不加入该分母。完整JMeter仪表板的TOTAL口径包含运营请求及测试运行窗口，可能与这里的业务吞吐不同。

## 分阶段结果

{table(['阶段','发压秒数','用户会话','业务HTTP数','观测HTTP请求/秒','HTTP P50/P95/P99 ms','429','售罄拒绝','非预期异常','新订单回执','订单可见P99 ms'],phase_rows)}

JMeter峰值活动线程（包含运营线程）：常态{normal['peak_active_jmeter_threads_including_operator']}、峰值{peak['peak_active_jmeter_threads_including_operator']}。这是观测到的活动用户线程数，不是配置的每秒新会话数，也不是同时在途HTTP数。

HTTP 5xx：常态{normal['http_5xx']}、峰值{peak['http_5xx']}；非HTTP错误：常态{normal['non_http_errors']}、峰值{peak['non_http_errors']}。UNKNOWN回执：常态{normal['unknown_receipts']}、峰值{peak['unknown_receipts']}。原始JMeter将429计为失败，报告明确列出，不能据非预期异常为零宣称所有业务请求成功。

## 接口分布与延迟

{table(['阶段','接口及结果','样本数','P50 ms','P95 ms','P99 ms','最大ms'],route_rows)}

## 缓存路径观测

本轮采样累计观察到{cache['requests']:,}次店铺详情读取（含就绪探测），L1命中{cache['l1Hits']:,}次、占比{cache['l1Hits']/cache['requests']:.2%}，L2查询{cache['l2Lookups']:,}次，数据库回源{cache['databaseFallbacks']:,}次。该场景覆盖实际冷读与L1过期后的L2读取，未把“零回源”设为通过条件。分阶段接口延迟以原始JTL为准，累计计数不混作某一个阶段的命中率。

两阶段的数据温度、到达率和限流比例不同，不构成缓存或下单算法的优化前后对照；峰值阶段部分接口P99较低也不代表容量更高。

## 最终业务对账

两阶段共收到{audit['receipt_count']:,}个新订单回执，数据库对应{audit['database_orders']:,}笔订单，编号、用户与券逐项匹配：{audit['receipt_ids_owners_vouchers_match']}。重复用户/券组合{audit['duplicate_user_voucher_groups']}组，最终状态与支付/取消/超时路径一致：{audit['expected_final_states_match']}。

最终订单状态分布（2=已支付，4=已取消）：`{audit['status_counts']}`。按未取消订单核对每张券的MySQL库存、Redis库存和资格集合，全部匹配：{all(x['stock_matches'] and x['qualified_users_match'] for x in audit['inventory'])}。未支付订单按原5分钟期限自然关闭，没有修改超时时间或通过SQL强制改状态。

待完成/待确认回执分别为{audit['pending_receipts']}/{audit['unconfirmed_receipts']}；持久化关单任务{audit['timeout_tasks']['persisted']}条，已确认{audit['timeout_tasks']['confirmed']}条。秒杀与关单消费者位点最终收敛：{all(x['converged'] for x in audit['mq'].values())}。详细库存与位点保留在[逐项对账]({evidence}reconciliation.json)。

本轮各阶段均无非预期HTTP异常：{result.get('all_phases_without_unexpected_http_failures')}。这与最终业务对账是否通过是两个不同判断；出现连接失败的档位不能只凭低P99或对账通过判为性能达标。

## 简历写法

{resume}

短版：使用JMeter验证浏览、抢券、查单及支付/取消混合负载，覆盖热点与长尾访问、重复提交、售罄和超时关单，完成订单、库存与MQ消费对账。

峰值阶段的吞吐包含明确的限流拒绝，不将其包装为同等数量的成功下单能力；也不使用本轮结果给出未经对照的缓存提升倍数。

## 复现与证据

```powershell
..\\agent-service\\.venv\\Scripts\\python.exe tests/load/run_business_jmeter.py
```

- [JMeter计划](../tests/load/business_mixed.jmx)、[运行器](../tests/load/run_business_jmeter.py)、[请求编排](../tests/load/business_prepare.groovy)、[业务断言](../tests/load/business_assert.groovy)。
- [常态原始JTL]({evidence}normal.jtl)、[峰值原始JTL]({evidence}peak.jtl)、[常态仪表板]({evidence}normal-dashboard/index.html)、[峰值仪表板]({evidence}peak-dashboard/index.html)。
- [阶段汇总]({evidence}phase-results.json)、[源代码与构建指纹]({evidence}versions.json)、[缓存计数]({evidence}cache-counters.jsonl)、[JVM与连接池]({evidence}jvm-metrics.jsonl)、[资源采样]({evidence}resource_usage.json)。
- [本轮实际编译源码及压测脚本快照]({evidence}measured-source.zip)、[快照指纹]({evidence}measured-source.json)。快照不含运行时令牌与密码文件。
- [本轮运行时Docker与Java/JMeter进程核对]({evidence}live-verification.json)、[原始JTL独立计数核对]({evidence}raw-jtl-crosscheck.json)。
- [测试资源清理]({evidence}cleanup_report.json)、[最终环境核对]({evidence}final_state.json)。原有数据卷保留：{final['normal_volumes_preserved']}。
- [最终端口与项目进程核对]({evidence}final-port-check.json)。

前置短测经历过运行器参数冲突和Groovy脚本错误，失败运行保留；修正后完成150个用户会话、501次业务HTTP请求的短测，之后使用全新隔离环境执行本轮正式测试。短测数据未拼入正式结果。
{previous}
'''
    (run/'LOCAL_BUSINESS_JMETER.md').write_text(text,encoding='utf-8')
    (ROOT/'docs/LOCAL_BUSINESS_JMETER.md').write_text(text,encoding='utf-8')
    (run/'resume-wording.txt').write_text(resume+'\n',encoding='utf-8')
    print(json.dumps({'report':str(ROOT/'docs/LOCAL_BUSINESS_JMETER.md'),'resume':resume},ensure_ascii=False))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run',type=Path)
    parser.add_argument('--prior',type=Path)
    args=parser.parse_args()
    resolved=args.run.resolve()
    if resolved.parent!=(ROOT/'artifacts/test-runs').resolve():
        raise ValueError('Expected a run directly under artifacts/test-runs')
    prior=args.prior.resolve() if args.prior else None
    if prior and prior.parent!=(ROOT/'artifacts/test-runs').resolve():
        raise ValueError('Prior evidence must also be a direct run directory')
    report(resolved,prior)
