"""Measure offered/confirmed/sold-out/backpressured seckill rates independently.

Only this disposable build raises global/IP limits; stock, user eligibility,
max-pending, Broker ACK and SQL transaction semantics remain unchanged.
"""
import argparse
import asyncio
import collections
import csv
import hashlib
import json
import math
import os
import re
import sys
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
from redis.asyncio import Redis
from run_business_jmeter import BusinessRun, ROOT, HERE, prop, node, script, http, headers, seed_business, percentile
from run_http_perf import patch_source, write


class CapacityRun(BusinessRun):
    def prepare_additional_source(self, work):
        java=work/'src/main/java/com/hmdp'
        path=java/'ratelimit/RateLimitAspect.java'
        patch_source(path, [('            buckets.add(new RateLimitBucket(key, rule.limit(), rule.windowSeconds() * 1000L));',
            '''            boolean capacityFixture = rateLimit.key().equals("voucher-seckill")
                    && request.getLocalAddr().equals("127.0.0.1")
                    && request.getRequestURI().matches("/voucher-order/seckill/98[12][0-9]{2}");
            int limit = capacityFixture && rule.scope() != RateLimitScope.USER ? 200000 : rule.limit();
            buckets.add(new RateLimitBucket(key, limit, rule.windowSeconds() * 1000L));''')])
        patch_source(java/'mq/SeckillOrderConsumer.java', [
            ('            voucherOrderService.createVoucherOrder(message);',
             '            voucherOrderService.createVoucherOrder(message);\n            log.info("CAPACITY_COMMITTED orderId={}, at={}", message.getOrderId(), System.currentTimeMillis());')])

    def create(self, role, image, options, command, config=None):
        if role=='redis':
            options=list(options)
            for flag in ('--memory','--memory-swap'):
                options[options.index(flag)+1]='384m'
            command=list(command)
            command[command.index('--maxmemory')+1]='256mb'
        return super().create(role,image,options,command,config=config)

    def build(self):
        super().build()
        path=self.path/'versions.json'
        data=json.loads(path.read_text(encoding='utf-8'))
        data.update(limiter_unchanged=False, production_files_unchanged=True,
            test_only_limits={'global_per_minute':200000,'ip_per_minute':200000,'user_per_minute':5},
            redis_memory_mib=384,redis_maxmemory_mib=256,
            semantics='Unchanged stock Lua, order SQL, Broker-confirmed acceptance and 2000 pending ceiling; isolated limit change and completion log only',
            capacity_sources={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in
                [HERE/'run_seckill_capacity.py',HERE/'capacity_prepare.groovy',HERE/'capacity_classify.groovy']})
        write(path,data)


def dataset(tree, name, file_property):
    item,_=node(tree,'CSVDataSet',name,'TestBeanGUI')
    for k,v in {'filename':'${__P('+file_property+')}', 'fileEncoding':'UTF-8','variableNames':'uid,token',
                'delimiter':',','shareMode':'shareMode.all'}.items():prop(item,k,v)
    for k,v in {'ignoreFirstLine':False,'quotedData':False,'recycle':False,'stopThread':True}.items():prop(item,k,v,'boolProp')


def sampler(tree):
    child=http(tree,'Seckill capacity','${request_path}','${request_method}')
    script(child,'JSR223PreProcessor','Prepare measured or warmup request',filename=HERE/'capacity_prepare.groovy')
    script(child,'JSR223PostProcessor','Classify actual business response',filename=HERE/'capacity_classify.groovy')


def make_plan():
    root=ET.Element('jmeterTestPlan',version='1.2',properties='5.0',jmeter='5.6.3')
    top=ET.SubElement(root,'hashTree')
    _,tree=node(top,'TestPlan','Seckill capacity: confirmed versus rejected','TestPlanGui')
    group,warm=node(tree,'SetupThreadGroup','Warm injector and order path','SetupThreadGroupGui')
    controller=ET.SubElement(group,'elementProp',name='ThreadGroup.main_controller',elementType='LoopController',guiclass='LoopControlPanel',testclass='LoopController',testname='Warmup')
    prop(controller,'LoopController.loops','32')
    prop(controller,'LoopController.continue_forever',False,'boolProp')
    prop(group,'ThreadGroup.num_threads','1');prop(group,'ThreadGroup.ramp_time','1')
    prop(group,'ThreadGroup.on_sample_error','continue')
    dataset(warm,'Warmup identities','warmFixtures');headers(warm,'${token}','127.0.0.1');sampler(warm)
    # A following one-shot setup group marks the start, after all warmup operations.
    marker,marktree=node(tree,'SetupThreadGroup','Warmup marker','SetupThreadGroupGui')
    controller=ET.SubElement(marker,'elementProp',name='ThreadGroup.main_controller',elementType='LoopController',guiclass='LoopControlPanel',testclass='LoopController',testname='Marker')
    prop(controller,'LoopController.loops','1');prop(controller,'LoopController.continue_forever',False,'boolProp')
    prop(marker,'ThreadGroup.num_threads','1');prop(marker,'ThreadGroup.ramp_time','1')
    script(marktree,'JSR223Sampler','warmup.marker',code="""long epoch = System.currentTimeMillis()
props.put('mainEpoch',Long.toString(epoch))
new File(props.get('clockFile')).text = Long.toString(epoch)
vars.put('bucket','warmup')""")
    # Setup groups normally run concurrently. Serialize the plan so the marker is after warmup.
    plan=top.find('TestPlan');prop(plan,'TestPlan.serialize_threadgroups',True,'boolProp')
    group,load=node(tree,'ThreadGroup','Persistent clients with shared throughput timer','ThreadGroupGui')
    controller=ET.SubElement(group,'elementProp',name='ThreadGroup.main_controller',elementType='LoopController',guiclass='LoopControlPanel',testclass='LoopController',testname='Loop')
    prop(controller,'LoopController.loops','-1');prop(controller,'LoopController.continue_forever',False,'boolProp')
    for k,v in {'num_threads':'400','ramp_time':'10','duration':'${__P(duration)}','delay':'0','on_sample_error':'continue'}.items():prop(group,'ThreadGroup.'+k,v)
    prop(group,'ThreadGroup.scheduler',True,'boolProp')
    pacing=script(load,'JSR223Sampler','Pace without recording a second HTTP request',code='SampleResult.setIgnore()')
    timer,_=node(pacing,'ConstantThroughputTimer','Shared request rate per group','TestBeanGUI')
    # Per-group (non-shared) divides rate across persistent clients, avoiding the
    # shared mode's rounded 1 ms inter-arrival interval at a 2000/s target.
    prop(timer,'calcMode','2','intProp');prop(timer,'throughput','${__P(perMinute)}')
    dataset(load,'One identity per offered request','fixtures');headers(load,'${token}','127.0.0.1');sampler(load)
    ET.indent(root)
    ET.ElementTree(root).write(HERE/'seckill_capacity.jmx',encoding='UTF-8',xml_declaration=True)


async def queue_probe(run, stop):
    async with Redis.from_url(run.redis_url,decode_responses=True) as redis:
        while not stop.is_set():
            row={'at':int(time.time()*1000),'pending':await redis.zcard('seckill:admission:pending'),
                 'unconfirmed':await redis.zcard('seckill:admission:unconfirmed')}
            with (run.path/'capacity-queue.jsonl').open('a',encoding='utf-8') as f:f.write(json.dumps(row)+'\n')
            try:await asyncio.wait_for(stop.wait(),.5)
            except TimeoutError:pass


def analyze(run, name, rate, seconds, result):
    with (run.path/(name+'.jtl')).open(encoding='utf-8-sig',newline='') as f:rows=list(csv.DictReader(f))
    epoch=int((run.path/(name+'-clock.txt')).read_text())
    start=epoch+10000; finish=start+seconds*1000
    measured=[r for r in rows if r['label'].startswith('steady.') and start<=int(r['timeStamp'])<finish]
    post=[r for r in rows if r['label'].startswith(('steady.','late.'))]
    accepted=[r for r in measured if r['outcome']=='accepted']
    receipts=[r for r in rows if r['outcome'] in ('accepted','unknown') and r.get('orderId') not in ('',None,'null')]
    types=collections.Counter(r['outcome'] for r in measured)
    markers={}
    for path in (run.path/'java.log',run.path/'java-restarted.log'):
        if path.exists():
            for key,stamp in re.findall(r'CAPACITY_COMMITTED orderId=(\d+), at=(\d+)',path.read_text(encoding='utf-8',errors='replace')):
                markers[key]=min(int(stamp),markers.get(key,int(stamp)))
    commit=[markers[r['orderId']]-int(r['timeStamp']) for r in accepted if r['orderId'] in markers]
    queue=[json.loads(s) for s in (run.path/'capacity-queue.jsonl').read_text().splitlines()]
    in_load=[q for q in queue if start<=q['at']<=finish]
    phase_ids={r['orderId'] for r in receipts if r['bucket'] in ('steady','late')}
    completions=sum(start<=stamp<finish for key,stamp in markers.items() if key in phase_ids)
    by_result={}
    for outcome in sorted(types):
        values=[int(r['elapsed']) for r in measured if r['outcome']==outcome]
        by_result[outcome]={'count':len(values),'p50_ms':percentile(values,.5),'p95_ms':percentile(values,.95),'p99_ms':percentile(values,.99)}
    data={'phase':name,'target_rps':rate,'steady_seconds':seconds,'measured_requests':len(measured),
          'delivered_start_rps':len(measured)/seconds,'confirmed_accept_rps':len(accepted)/seconds,
          'window_start_ms':start,'window_end_ms':finish,'late_requests':sum(r['bucket']=='late' for r in rows),
          'steady_handler_completions':completions,'steady_handler_completion_rps':completions/seconds,
          'counts':dict(types),'by_result':by_result,'jmeter_exit':result.returncode,
          'all_warmup_ramp_and_steady_samples':len(rows),'all_business_receipts':len(receipts),
          'steady_receipts':len(accepted),'steady_handler_marker_coverage':len(commit),
          'request_to_handler_return_ms':{'p50':percentile(commit,.5),'p95':percentile(commit,.95),'p99':percentile(commit,.99)},
          'queue_peak':max((q['pending'] for q in in_load),default=0),
          'queue_near_window_end':in_load[-1] if in_load else None,
          'queue_at_end':queue[-1] if queue else None,
          'peak_jmeter_threads':max((int(r.get('allThreads') or 0) for r in post),default=0),
          'unexpected_examples':[{k:r.get(k) for k in ('responseCode','responseMessage','uid','orderId')} for r in measured if r['outcome']=='unexpected'][:6]}
    data['target_delivery_met']=rate*.98<=data['delivered_start_rps']<=rate*1.02
    data['injector_cap_hit']=(run.path/(name+'-injector-cap.txt')).exists()
    data['all_eligible_acceptance_met']=data['target_delivery_met'] and types.get('accepted',0)==len(measured) and bool(measured)
    write(run.path/(name+'-summary.json'),data)
    return data,receipts


async def phase(run,args,spec,next_uid):
    name,rate,seconds,voucher,stock=spec
    await run.coupon(voucher,stock)
    count=int(rate*(seconds+10)*1.35)+500
    fixture=run.path/'temp'/(name+'.csv')
    with fixture.open('w',encoding='utf-8',newline='') as f:csv.writer(f).writerows((uid,run.tokens[uid]) for uid in range(next_uid,next_uid+count))
    warm=run.path/'temp/warm.csv'
    with warm.open('w',encoding='utf-8',newline='') as f:csv.writer(f).writerows((uid,run.tokens[uid]) for uid in range(10001,10033))
    properties=run.path/'temp'/(name+'.properties')
    properties.write_text('\n'.join(f'{k}={v}' for k,v in {'port':run.java_port,'rate':rate,'seconds':seconds,
        'duration':seconds+11,'perMinute':rate*60,
        'voucher':voucher,'warmVoucher':98100,'warmFixtures':warm.as_posix(),'fixtures':fixture.as_posix(),
        'clockFile':(run.path/(name+'-clock.txt')).as_posix(),
        'capFile':(run.path/(name+'-injector-cap.txt')).as_posix(),
        'sample_variables':'uid,orderId,outcome,bucket','jmeter.save.saveservice.output_format':'csv',
        'jmeter.save.saveservice.response_data':'false','jmeter.save.saveservice.print_field_names':'true',
        'jmeterengine.force.system.exit':'true'}.items()),encoding='utf-8')
    run.stage('capacity-'+name)
    write(run.path/(name+'-frozen.json'),{'target_rps':rate,'seconds':seconds,'stock':stock,'voucher':voucher,
        'identity_capacity':count,'warmup':'32 actual order requests, then 10 seconds injector GET ramp; distinct warmup voucher',
        'injector':'400 persistent threads, shared ConstantThroughputTimer; actual achieved rate reported; not an unlimited open-arrival capacity claim',
        'test_limits_global_ip_user_per_minute':[200000,200000,5],'max_pending_unchanged':2000})
    result=await asyncio.to_thread(run.command,[args.jmeter.resolve(),'-n','-t',HERE/'seckill_capacity.jmx','-q',properties,
        '-l',run.path/(name+'.jtl'),'-j',run.path/(name+'-jmeter.log'),'-e','-o',run.path/(name+'-dashboard')],
        timeout=seconds+120,check=False,env=dict(run.env,HEAP='-Xms256m -Xmx768m -XX:MaxMetaspaceSize=256m',JVM_ARGS='-Xss256k'))
    (run.path/(name+'-console.log')).write_text(result.stdout+result.stderr,encoding='utf-8')
    # Drain existing accepted work before the next tier; do not let backlog contaminate a later tier.
    stop_at=time.monotonic()+120
    async with Redis.from_url(run.redis_url,decode_responses=True) as redis:
        while time.monotonic()<stop_at:
            pending=await redis.zcard('seckill:admission:pending')
            if not pending:break
            print('CAPACITY_DRAIN '+name+' '+str(pending),flush=True)
            await asyncio.sleep(3)
    data,receipts=analyze(run,name,rate,seconds,result)
    snap=await run.snapshot_voucher(voucher)
    actual={(str(o['id']),o['user_id']) for o in snap['orders']}
    expected={(r['orderId'],int(r['uid'])) for r in receipts if int(r['uid'])>=10101}
    # Warmup receipts reference another voucher, so exclude them by bucket.
    expected={(r['orderId'],int(r['uid'])) for r in receipts if r['bucket'] in ('steady','late')}
    active=[o for o in snap['orders'] if o['status']!=4]
    audit={'database_orders':len(actual),'receipt_orders':len(expected),'ids_and_users_match':actual==expected,
        'mysql_stock':snap['mysql_stock'],'redis_stock':snap['redis_stock'],'expected_stock':stock-len(active),
        'stock_matches':snap['mysql_stock']==snap['redis_stock']==stock-len(active)>=0,
        'qualification_matches':set(snap['redis_qualified_users'])=={str(o['user_id']) for o in active},
        'last_pending':pending,'statuses':dict(collections.Counter(o['status'] for o in snap['orders']))}
    audit['pass']=audit['ids_and_users_match'] and audit['stock_matches'] and audit['qualification_matches'] and pending==0
    data['audit']=audit
    write(run.path/(name+'-summary.json'),data)
    print('CAPACITY_RESULT '+json.dumps({k:data[k] for k in ['phase','target_rps','delivered_start_rps','confirmed_accept_rps','counts','target_delivery_met','all_eligible_acceptance_met','audit']},ensure_ascii=False),flush=True)
    return data,next_uid+count


async def main(args):
    make_plan()
    specs=[('smoke',50,10,98101,10000)] if args.smoke else [
        ('eligible-200',200,20,98101,100000),('eligible-500',500,20,98102,100000),
        ('eligible-1000',1000,20,98103,100000),('eligible-2000',2000,30,98104,100000),
        ('scarce-2000',2000,30,98105,1000)]
    users=sum(int(rate*(seconds+10)*1.35)+500 for _,rate,seconds,_,_ in specs)+500
    run=CapacityRun()
    print('SECKILL_CAPACITY_RUN '+str(run.path),flush=True)
    rows=[];stop=asyncio.Event();probe=None
    write(run.path/'capacity-plan.json',{'tiers':specs,'fixture_users':users,'production_limits_modified':False,
        'injector':'400 persistent threads with shared throughput timer and 10-second ramp; underdelivery remains a failed target',
        'isolated_global_ip_limit_per_minute':200000,'stock_and_ack_and_mysql_checks_retained':True,
        'all_eligible_goal':'2000 new qualified orders per second offered; report rejection and queue growth honestly',
        'scarce_goal':'2000 contenders per second for 1000 coupons; not 2000 successful orders per second'})
    try:
        run.sample('before',guard=True);run.build();await run.infrastructure()
        await seed_business(run,users)
        # Independently retain row counts rather than only the declared fixture sizes.
        write(run.path/'fixture-counts-sql.json',run.sql('SELECT (SELECT COUNT(*) FROM tb_shop),(SELECT COUNT(*) FROM tb_voucher_order),(SELECT COUNT(*) FROM tb_user);'))
        await run.coupon(98100,32)
        run.monitor_thread=threading.Thread(target=run.monitor,daemon=True);run.monitor_thread.start()
        probe=asyncio.create_task(queue_probe(run,stop));uid=10101
        for spec in specs:
            row,uid=await phase(run,args,spec,uid);rows.append(row);write(run.path/'capacity-results.json',rows)
        write(run.path/'capacity-final.json',{'run':run.id,'tiers':rows,'all_order_audits_pass':all(r['audit']['pass'] for r in rows),
             'goal_2000_all_eligible_met':any(r['phase']=='eligible-2000' and r['all_eligible_acceptance_met'] for r in rows),
             'notes':'HTTP total, Broker-confirmed acceptance and SQL completion are separate; ramp and setup excluded from steady numbers'})
    except BaseException as error:
        run.failure=type(error).__name__+': '+str(error);write(run.path/'capacity-failure.json',{'error':run.failure});raise
    finally:
        stop.set()
        if probe:await probe
        run.cleanup();print('SECKILL_CAPACITY_FINISHED '+str(run.path),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jmeter',type=Path,default=ROOT/'artifacts/test-runs/.tools/apache-jmeter-5.6.3/bin/jmeter.bat')
    parser.add_argument('--smoke',action='store_true')
    asyncio.run(main(parser.parse_args()))
