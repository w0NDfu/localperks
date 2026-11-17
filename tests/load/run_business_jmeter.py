"""Local JMeter journeys, real HTTP and disk-backed owned middleware; no model calls.

Keep normal application logic and rate limits. Export raw JTL, per-route latency,
order visibility, expected rejections, and SQL/Redis/MQ reconciliation separately.
"""
import argparse
import asyncio
import collections
import csv
import hashlib
import json
import math
import os
import shutil
import subprocess
import threading
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
from redis.asyncio import Redis
from run_http_perf import HttpPerfRun, ROOT, HERE, patch_source, write
from run_k6 import seed_users


def prop(parent, name, value, kind='stringProp'):
    ET.SubElement(parent, kind, name=name).text = str(value).lower() if isinstance(value, bool) else str(value)


def node(tree, tag, name, gui=None):
    element = ET.SubElement(tree, tag, guiclass=gui or tag + 'Gui', testclass=tag, testname=name, enabled='true')
    return element, ET.SubElement(tree, 'hashTree')


def script(tree, tag, name, code=None, filename=None):
    item, children = node(tree, tag, name, 'TestBeanGUI')
    prop(item, 'scriptLanguage', 'groovy')
    prop(item, 'cacheKey', 'true')
    prop(item, 'filename', str(filename) if filename else '')
    prop(item, 'script', code or '')
    return children


def http(tree, name, path, method='GET', body=''):
    item, children = node(tree, 'HTTPSamplerProxy', name, 'HttpTestSampleGui')
    arguments = ET.SubElement(item, 'elementProp', name='HTTPsampler.Arguments', elementType='Arguments')
    collection = ET.SubElement(arguments, 'collectionProp', name='Arguments.arguments')
    argument = ET.SubElement(collection, 'elementProp', name='', elementType='HTTPArgument')
    prop(argument, 'HTTPArgument.always_encode', False, 'boolProp')
    prop(argument, 'Argument.value', body)
    prop(argument, 'Argument.metadata', '=')
    for key, value in {'domain':'127.0.0.1', 'port':'${__P(port)}', 'protocol':'http', 'path':path,
                       'method':method, 'connect_timeout':'3000', 'response_timeout':'10000'}.items():
        prop(item, 'HTTPSampler.' + key, value)
    for key, value in {'follow_redirects':False, 'auto_redirects':False, 'use_keepalive':True, 'postBodyRaw':True}.items():
        prop(item, 'HTTPSampler.' + key, value, 'boolProp')
    return children


def headers(tree, token, ip):
    item, _ = node(tree, 'HeaderManager', 'Identity and JSON headers', 'HeaderPanel')
    collection = ET.SubElement(item, 'collectionProp', name='HeaderManager.headers')
    for name, value in [('Authorization', token), ('X-Forwarded-For', ip), ('Content-Type', 'application/json')]:
        entry = ET.SubElement(collection, 'elementProp', name='', elementType='Header')
        prop(entry, 'Header.name', name)
        prop(entry, 'Header.value', value)


def make_plan(path):
    root = ET.Element('jmeterTestPlan', version='1.2', properties='5.0', jmeter='5.6.3')
    tree = ET.SubElement(root, 'hashTree')
    plan, tree = node(tree, 'TestPlan', 'Local mixed business journeys', 'TestPlanGui')
    prop(plan, 'TestPlan.comments', '90% browsing sessions; 6% pay, 2% cancel, 1% abandon, 1% scarce. Assumed workload, not production traffic replay.')
    group, children = node(tree, 'org.apache.jmeter.threads.openmodel.OpenModelThreadGroup', 'Offered customer sessions',
                           'org.apache.jmeter.threads.openmodel.gui.OpenModelThreadGroupGui')
    prop(group, 'OpenModelThreadGroup.schedule', 'rate(${__P(rate)}/sec) random_arrivals(${__P(seconds)} sec) rate(${__P(rate)}/sec) pause(35 sec)')
    prop(group, 'OpenModelThreadGroup.random_seed', '5107', 'longProp')
    prop(group, 'ThreadGroup.on_sample_error', 'continue')
    data, _ = node(children, 'CSVDataSet', 'One authenticated synthetic user per session', 'TestBeanGUI')
    for key, value in {'filename':'${__P(fixtures)}', 'fileEncoding':'UTF-8', 'variableNames':'uid,token',
                       'delimiter':',', 'shareMode':'shareMode.all'}.items():
        prop(data, key, value)
    for key, value in {'ignoreFirstLine':False, 'quotedData':False, 'recycle':False, 'stopThread':True}.items():
        prop(data, key, value, 'boolProp')
    headers(children, '${token}', '${clientIp}')
    controller, loop = node(children, 'WhileController', 'Browse then transact and verify', 'WhileControllerGui')
    prop(controller, 'WhileController.condition', "${__groovy(vars.get('done') != 'true')}")
    request = http(loop, 'Business request', '${request_path}', '${request_method}', '${request_body}')
    script(request, 'JSR223PreProcessor', 'Prepare next business step', filename=HERE / 'business_prepare.groovy')
    script(request, 'JSR223Timer', 'Customer think time', code="return Long.parseLong(vars.get('pause_ms') ?: '0')")
    script(request, 'JSR223PostProcessor', 'Validate business state and advance', filename=HERE / 'business_assert.groovy')
    update, update_tree = node(tree, 'ThreadGroup', 'Shop operator updates', 'ThreadGroupGui')
    loop_config = ET.SubElement(update, 'elementProp', name='ThreadGroup.main_controller', elementType='LoopController', guiclass='LoopControlPanel', testclass='LoopController', testname='Loop')
    prop(loop_config, 'LoopController.continue_forever', False, 'boolProp')
    prop(loop_config, 'LoopController.loops', '-1')
    for key, value in {'num_threads':'1', 'ramp_time':'1', 'duration':'${__P(seconds)}', 'delay':'0', 'on_sample_error':'continue'}.items():
        prop(update, 'ThreadGroup.' + key, value)
    prop(update, 'ThreadGroup.scheduler', True, 'boolProp')
    headers(update_tree, '${__P(adminToken)}', '198.19.0.1')
    request = http(update_tree, 'admin.shop.update', '/shop', 'PUT', '${update_body}')
    script(request, 'JSR223PreProcessor', 'Change a hot shop address', code="""import groovy.json.JsonOutput
int n = Integer.parseInt(vars.get('update_n') ?: '0') + 1
vars.put('update_n', n.toString())
vars.put('update_body', JsonOutput.toJson([id:100000 + n % 50, address:'updated-' + props.get('phaseSeed') + '-' + n]))
""")
    timer, _ = node(request, 'ConstantTimer', 'One operator change every five seconds', 'ConstantTimerGui')
    prop(timer, 'ConstantTimer.delay', '5000')
    script(request, 'JSR223PostProcessor', 'Verify update acknowledgement', code="""import groovy.json.JsonSlurper
try { def b = new JsonSlurper().parseText(prev.responseDataAsString); prev.setSuccessful(prev.responseCode == '200' && b.success == true) }
catch(Exception ignored) { prev.setSuccessful(false) }
""")
    ET.indent(root)
    ET.ElementTree(root).write(path, encoding='UTF-8', xml_declaration=True)


class BusinessRun(HttpPerfRun):
    def __init__(self):
        super().__init__(100, 'local-business-jmeter', arrivals=True)
        self.storage_profile = 'disk'
        self.env.update(HMDP_OPS_ENABLED='false', HMDP_RATE_LIMIT_TRUST_FORWARDED_HEADERS='true',
                        SHOPLINK_CACHE_METRICS=str(self.path / 'cache-counters.jsonl'))

    def build(self):
        self.stage('build-original-logic-with-read-counters')
        work = self.path / 'temp/isolated-build'
        shutil.copytree(ROOT / 'src/main', work / 'src/main')
        shutil.copy2(ROOT / 'pom.xml', work / 'pom.xml')
        java = work / 'src/main/java/com/hmdp'
        (java / 'perf').mkdir(exist_ok=True)
        shutil.copy2(HERE / 'CacheReadProbe.java', java / 'perf/CacheReadProbe.java')
        patch_source(java / 'cache/ShopCacheService.java', [
            ('        if (!bloomFilterService.mightContain(id)) {', '        com.hmdp.perf.CacheReadProbe.requests.increment();\n        if (!bloomFilterService.mightContain(id)) {'),
            ('        if (localShop != null) {\n            return localShop;', '        if (localShop != null) {\n            com.hmdp.perf.CacheReadProbe.l1Hits.increment();\n            return localShop;'),
            ('        Shop shop = cacheClient.queryWithPassThrough(', '        com.hmdp.perf.CacheReadProbe.l2Lookups.increment();\n        Shop shop = cacheClient.queryWithPassThrough('),
            ('CACHE_SHOP_KEY, id, Shop.class, dbFallback, CACHE_SHOP_TTL, TimeUnit.MINUTES);',
             'CACHE_SHOP_KEY, id, Shop.class, item -> { com.hmdp.perf.CacheReadProbe.databaseFallbacks.increment(); return dbFallback.apply(item); }, CACHE_SHOP_TTL, TimeUnit.MINUTES);'),
        ])
        # Metrics only; the real rate limiter, services, SQL, Lua and transaction logic are unchanged.
        shutil.copy2(HERE / 'PerfProbe.java', java / 'perf/PerfProbe.java')
        self.prepare_additional_source(work)
        result = self.command([shutil.which('mvn.cmd'), '-o', '-B', '-f', work / 'pom.xml', '-Dmaven.test.skip=true', 'package'],
                              timeout=180, check=False, env=dict(self.env, MAVEN_OPTS='-Xms64m -Xmx512m'))
        (self.path / 'maven.log').write_text(result.stdout + result.stderr, encoding='utf-8')
        if result.returncode:
            raise RuntimeError('Isolated build failed; see maven.log')
        self.jar = work / 'target/localperks-backend-0.0.1-SNAPSHOT.jar'
        write(self.path / 'versions.json', {'java':self.command(['java', '-version']).stderr,
            'jar_sha256':hashlib.sha256(self.jar.read_bytes()).hexdigest(),
            'normal_volumes_before':self.command(['docker', 'volume', 'ls', '--format', '{{.Name}}']).stdout.splitlines(),
            'production_logic_unchanged':True, 'limiter_unchanged':True,
            'source_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in [ROOT/'pom.xml', HERE/'run_business_jmeter.py', HERE/'business_prepare.groovy', HERE/'business_assert.groovy']}})

    def prepare_additional_source(self, work):
        """Optional changes confined to the manifest-owned test build."""
        pass


async def seed_business(run, total_sessions):
    run.stage('seed-20000-shops-and-100000-historical-orders')
    await seed_users(run, total_sessions + 200)
    run.sql("INSERT INTO tb_shop_type(id,name,sort) VALUES" + ','.join(f"({i},'Category{i}',{i})" for i in range(2,11)))
    for start in range(0, 20000, 1000):
        run.sql('INSERT INTO tb_shop(id,name,type_id,address,x,y,avg_price,open_hours) VALUES' + ','.join(
            f"({100000+i},'MixShop_{i}',{1+i%10},'Fixture address',{120.1+(i%200)*.0001},{30.2+(i//200)*.0001},{40+i%160},'09:00-23:00')"
            for i in range(start,start+1000)))
    run.sql('INSERT INTO tb_voucher(id,shop_id,title,rules,pay_value,actual_value,type,status) VALUES' + ','.join(
        f"({200000+i},{100000+i},'General voucher {i}','长期有效；全天可用；每桌限1张',6000,10000,0,1)" for i in range(1000)))
    for start in range(0,10000,1000):
        run.sql('INSERT INTO tb_user(id,phone,nick_name) VALUES' + ','.join(
            f"({1000000+i},'177{i:08d}','Historical user')" for i in range(start,start+1000)))
    for start in range(0,100000,2000):
        run.sql('INSERT INTO tb_voucher_order(id,user_id,voucher_id,status,create_time,pay_time) VALUES' + ','.join(
            f"({1+i},{1000000+i//10},{200000+i%10},2,DATE_SUB(NOW(),INTERVAL 30 DAY),DATE_SUB(NOW(),INTERVAL 30 DAY))"
            for i in range(start,start+2000)))
    async with Redis.from_url(run.redis_url) as redis:
        pipe = redis.pipeline(transaction=False)
        for i in range(20000):
            pipe.geoadd('shop:geo:'+str(1+i%10), [120.1+(i%200)*.0001,30.2+(i//200)*.0001,str(100000+i)])
        await pipe.execute()
    # Startup Bloom calibration ran before fixture extension. Stop/restart just the owned Java process
    # so all 20,000 shops are included by the application's normal initialization path.
    run.processes[0].terminate()
    await asyncio.to_thread(run.processes[0].wait, 20)
    # Reuse the actual command line already launched by Run.infrastructure, with exactly the same environment.
    command = run.processes[0].args
    stream = (run.path / 'java-restarted.log').open('w', encoding='utf-8')
    run.files.append(stream)
    restarted = subprocess.Popen(command, cwd=ROOT, env=run.env, stdout=stream, stderr=subprocess.STDOUT)
    run.processes.append(restarted)
    async with httpx.AsyncClient(base_url=run.java_url, timeout=5, trust_env=False) as client:
        for _ in range(90):
            if restarted.poll() is not None:
                raise RuntimeError('Seeded Java restart failed')
            try:
                response = await client.get('/shop/119999')
                if response.status_code == 200 and response.json().get('success'):
                    break
            except Exception:
                pass
            await asyncio.sleep(1)
        else:
            raise RuntimeError('Seeded Java readiness timeout')
    write(run.path / 'fixtures.json', {'shops':20003,'historical_orders':100000,'general_vouchers':1000,
        'historical_users':10000,'session_user_capacity':total_sessions,'warmup':'only readiness checks, no all-shop preheating',
        'shop_distribution':'80% of selected IDs from 200 hot shops, 20% from 19800 long-tail shops',
        'session_distribution':'90% browsing; 6% pay; 2% cancel; 1% unpaid timeout; 1% scarce-stock purchase',
        'data_source':'synthetic scenario assumptions, not captured production traffic'})


def percentile(values, p):
    return sorted(values)[max(0, math.ceil(len(values)*p)-1)] if values else None


def summarize(jtl, seconds):
    with jtl.open(encoding='utf-8-sig', newline='') as stream:
        rows = list(csv.DictReader(stream))
    business = [r for r in rows if not r['label'].startswith('admin.')]
    unexpected = [r for r in rows if r.get('outcome') == 'unexpected' or
                  (r['success'] != 'true' and not r['label'].endswith('.rate_limited'))]
    per_route = {}
    for label in sorted({r['label'] for r in rows}):
        items = [r for r in rows if r['label']==label]
        durations = [int(r['elapsed']) for r in items]
        per_route[label] = {'count':len(items),'p50_ms':percentile(durations,.5),'p95_ms':percentile(durations,.95),
                            'p99_ms':percentile(durations,.99),'max_ms':max(durations)}
    start = min(int(r['timeStamp']) for r in business)
    end = max(int(r['timeStamp'])+int(r['elapsed']) for r in business)
    last_start = start + seconds*1000
    in_window = [r for r in business if start <= int(r['timeStamp']) < last_start]
    sessions = [r for r in business if r['label'].startswith('shop.search.')]
    completed = [r for r in business if r.get('session_ms') not in (None,'','null')]
    accepted = [r for r in business if r['label'] in ('seckill.submit.accepted','seckill.submit.unknown')]
    visible = [int(r['visible_ms']) for r in business if r.get('visible_ms') not in (None,'','null')]
    return {'http_samples':len(business),'operator_samples':len(rows)-len(business),
        'started_sessions':len(sessions),'finished_sessions':len(completed),
        'actual_session_starts_per_second':len(sessions)/seconds,
        'journey_counts':dict(collections.Counter(r['journey'] for r in sessions)),
        'configured_seconds':seconds,'observed_business_window_ms':end-start,
        'completed_response_rps_including_drain':len(business)*1000/(end-start),
        'requests_started_in_load_window':len(in_window),'request_start_rps_in_load_window':len(in_window)/seconds,
        'latency_ms':{k:percentile([int(r['elapsed']) for r in business],p) for k,p in [('p50',.5),('p95',.95),('p99',.99)]},
        'unexpected_count':len(unexpected),'unexpected_examples':[{k:r.get(k) for k in ('label','responseCode','responseMessage','uid','orderId')} for r in unexpected[:12]],
        'rate_limited':sum(r['label'].endswith('.rate_limited') for r in rows),
        'http_5xx':sum(r['responseCode'].isdigit() and 500 <= int(r['responseCode']) < 600 for r in rows),
        'non_http_errors':sum(not r['responseCode'].isdigit() for r in rows),
        'peak_active_jmeter_threads_including_operator':max(int(r.get('allThreads') or 0) for r in rows),
        'sold_out':sum(r.get('outcome')=='sold_out' for r in rows),
        'new_order_receipts':len(accepted),'distinct_order_receipts':len({r['orderId'] for r in accepted}),
        'unknown_receipts':sum(r['label']=='seckill.submit.unknown' for r in rows),
        'submit_to_visible_ms':{'count':len(visible),'p50':percentile(visible,.5),'p95':percentile(visible,.95),'p99':percentile(visible,.99),
                                'scope':'HTTP submit start to first successful status observation; includes think/poll interval'},
        'routes':per_route,'jtl_rows':len(rows)}


async def execute_phase(run, args, phase, start_uid):
    name, rate, seconds, voucher_base = phase
    run.stage('jmeter-' + name)
    for i in range(9):
        await run.coupon(voucher_base+i,20 if i==8 else 10000,shop=100000+i)
    count = max(200,int(rate*seconds*1.5)+200)
    fixture = run.path / 'temp' / (name + '-accounts.csv')
    with fixture.open('w',encoding='utf-8',newline='') as stream:
        csv.writer(stream).writerows((uid,run.tokens[uid]) for uid in range(start_uid,start_uid+count))
    jtl = run.path / (name + '.jtl')
    properties = run.path / 'temp' / (name + '.properties')
    properties.write_text('\n'.join(f'{k}={v}' for k,v in {
        'port':run.java_port,'rate':rate,'seconds':seconds,'voucherBase':voucher_base,
        'phaseSeed':5107+voucher_base,'fixtures':fixture.as_posix(),'adminToken':run.tokens[10001],
        'sample_variables':'journey,uid,voucherId,orderId,outcome,visible_ms,session_ms',
        'jmeter.save.saveservice.response_data':'false','jmeter.save.saveservice.samplerData':'false',
        'jmeter.save.saveservice.requestHeaders':'false','jmeter.save.saveservice.responseHeaders':'false',
        'jmeter.save.saveservice.print_field_names':'true','jmeter.save.saveservice.output_format':'csv',
        'jmeterengine.force.system.exit':'true',
    }.items()),encoding='utf-8')
    write(run.path / (name+'-frozen.json'), {'session_rate':rate,'seconds':seconds,'voucher_base':voucher_base,
        'default_limits':{'seckill_global_per_minute':300,'seckill_ip_per_minute':30,'seckill_user_per_minute':5},
        'forwarded_client_ips':'synthetic identities honored only by this loopback test process',
        'operator_update_interval_seconds':5,'fixture_users':count})
    command = [args.jmeter.resolve(),'-n','-t',HERE/'business_mixed.jmx','-q',properties,'-l',jtl,
               '-j',run.path/(name+'-jmeter.log'),'-e','-o',run.path/(name+'-dashboard')]
    started = time.monotonic()
    result = await asyncio.to_thread(run.command,command,timeout=seconds+120,check=False,
        env=dict(run.env,HEAP='-Xms128m -Xmx512m -XX:MaxMetaspaceSize=192m',JVM_ARGS='-Xss256k'))
    (run.path/(name+'-console.log')).write_text(result.stdout+result.stderr,encoding='utf-8')
    if not jtl.exists():
        raise RuntimeError('JMeter produced no JTL: '+name)
    summary = summarize(jtl,seconds)
    summary.update(name=name,offered_session_rate=rate,jmeter_exit=result.returncode,
                   wall_seconds=time.monotonic()-started,html_report=(run.path/(name+'-dashboard/index.html')).exists())
    summary['no_unexpected_http_failures'] = summary['unexpected_count'] == 0
    write(run.path/(name+'-summary.json'),summary)
    print('PHASE_RESULT '+json.dumps({k:summary[k] for k in ['name','http_samples','started_sessions','unexpected_count','rate_limited','new_order_receipts','submit_to_visible_ms']},ensure_ascii=False),flush=True)
    if result.returncode:
        raise RuntimeError('JMeter runner failed; inspect '+name+'-summary.json')
    if summary['unexpected_count']:
        # Keep failed samples and finish transaction reconciliation before teardown.
        print('HTTP_FAILURES_RETAINED '+name+' '+str(summary['unexpected_count']),flush=True)
    return summary, start_uid+count


async def reconcile(run, summaries, phases, drain_seconds):
    run.stage('wait-for-five-minute-unpaid-order-expiry')
    vouchers = [base+i for _,_,_,base in phases for i in range(9)]
    where = ','.join(map(str,vouchers))
    stop_at = time.monotonic()+drain_seconds
    while True:
        state = run.sql(f"SELECT COUNT(*),COALESCE(SUM(status=1),0) FROM tb_voucher_order WHERE voucher_id IN ({where});")[0]
        print('DRAIN '+json.dumps({'orders':int(state[0]),'unpaid':int(state[1]),'remaining_seconds':max(0,int(stop_at-time.monotonic()))}),flush=True)
        if int(state[1])==0 or time.monotonic()>=stop_at:
            break
        await asyncio.sleep(20)
    orders = run.sql_json(f"SELECT JSON_OBJECT('id',CAST(id AS CHAR),'user_id',user_id,'voucher_id',voucher_id,'status',status) FROM tb_voucher_order WHERE voucher_id IN ({where}) ORDER BY id;")
    counts = collections.Counter(o['status'] for o in orders)
    receipt_rows=[]
    expected_states={}
    for summary in summaries:
        with (run.path/(summary['name']+'.jtl')).open(encoding='utf-8-sig',newline='') as stream:
            for row in csv.DictReader(stream):
                if row['label'] in ('seckill.submit.accepted','seckill.submit.unknown'):
                    receipt_rows.append(row)
                    expected_states[row['orderId']]=2 if row['journey'] in ('pay','scarce') else 4
    receipts={(row['orderId'],int(row['uid']),int(row['voucherId'])) for row in receipt_rows}
    actual={(o['id'],o['user_id'],o['voucher_id']) for o in orders}
    snapshots=[]
    async with Redis.from_url(run.redis_url,decode_responses=True) as redis:
        for voucher in vouchers:
            rows=[o for o in orders if o['voucher_id']==voucher]
            active=[o for o in rows if o['status']!=4]
            mysql=int(run.sql(f'SELECT stock FROM tb_seckill_voucher WHERE voucher_id={voucher}')[0][0])
            stock=int(await redis.get('seckill:stock:'+str(voucher)))
            users=set(await redis.smembers('seckill:order:'+str(voucher)))
            initial=20 if voucher%10==8 else 10000
            snapshots.append({'voucher':voucher,'orders':len(rows),'active_orders':len(active),'mysql_stock':mysql,
                'redis_stock':stock,'expected_stock':initial-len(active),
                'stock_matches':mysql==stock==initial-len(active)>=0,
                'qualified_users_match':users=={str(o['user_id']) for o in active}})
        pending=int(await redis.zcard('seckill:admission:pending'))
        unconfirmed=int(await redis.zcard('seckill:admission:unconfirmed'))
    duplicates=int(run.sql(f'SELECT COUNT(*) FROM (SELECT user_id,voucher_id FROM tb_voucher_order WHERE voucher_id IN ({where}) GROUP BY user_id,voucher_id HAVING COUNT(*)>1) d;')[0][0])
    timeout_rows=run.sql(f"SELECT COUNT(*),COALESCE(SUM(status=1),0) FROM tb_mq_message WHERE topic='voucher-order-timeout-topic' AND voucher_id IN ({where});")[0]
    mq={}
    for topic,group in [('seckill-order-topic','hmdp-seckill-order-consumer'),('voucher-order-timeout-topic','hmdp-order-timeout-consumer')]:
        result=await asyncio.to_thread(run.admin,['consumerProgress','-n','namesrv:9876','-g',group],topic+'-final-progress')
        queues=[line.split() for line in result.stdout.splitlines() if line.startswith(topic+' ')]
        totals=[sum(int(q[c]) for q in queues) for c in range(3,7)] if queues else []
        mq[topic]={'columns_broker_consumer_lag_inflight':totals,'converged':len(totals)==4 and totals[0]==totals[1] and totals[2:]==[0,0]}
    audit={'receipt_count':len(receipt_rows),'database_orders':len(orders),'status_counts':dict(counts),
        'receipt_ids_owners_vouchers_match':receipts==actual,'duplicate_user_voucher_groups':duplicates,
        'expected_final_states_match':all(expected_states.get(o['id'])==o['status'] for o in orders),
        'pending_receipts':pending,'unconfirmed_receipts':unconfirmed,'inventory':snapshots,
        'timeout_tasks':{'persisted':int(timeout_rows[0]),'confirmed':int(timeout_rows[1])},'mq':mq}
    audit['pass']=(receipts==actual and len(receipt_rows)==len(orders) and duplicates==0 and
        audit['expected_final_states_match'] and pending==unconfirmed==0 and
        all(s['stock_matches'] and s['qualified_users_match'] for s in snapshots) and
        int(timeout_rows[0])==int(timeout_rows[1])==len(orders) and all(v['converged'] for v in mq.values()))
    write(run.path/'reconciliation.json',audit)
    write(run.path/'order-final-states.json',orders)
    return audit


async def main(args):
    make_plan(HERE/'business_mixed.jmx')
    phases=[('smoke',10,15,98400)] if args.smoke else [('normal',30,300,98400),('peak',100,180,98410)]
    capacity=sum(int(rate*seconds*1.5)+200 for _,rate,seconds,_ in phases)+500
    run=BusinessRun()
    print('BUSINESS_RUN '+str(run.path),flush=True)
    summaries=[]
    try:
        run.sample('before',guard=True)
        run.build()
        await run.infrastructure()
        await seed_business(run,capacity)
        run.monitor_thread=threading.Thread(target=run.monitor,daemon=True)
        run.monitor_thread.start()
        next_uid=10101
        for phase in phases:
            summary,next_uid=await execute_phase(run,args,phase,next_uid)
            summaries.append(summary)
            write(run.path/'phase-results.json',summaries)
        audit=({'pass':None,'scope':'API/journey smoke only; delayed-message expiry is verified in the full run'}
               if args.smoke else await reconcile(run,summaries,phases,360))
        write(run.path/'business-results.json',{'run':run.id,'phases':summaries,'audit_pass':audit['pass'],
            'all_phases_without_unexpected_http_failures':all(s['unexpected_count']==0 for s in summaries),
            'model_calls':0,'scope':'single-host real HTTP JMeter mixed business, assumed traffic distribution; no production claim'})
        if not args.smoke and not audit['pass']:
            raise RuntimeError('Business reconciliation failed')
    except BaseException as error:
        run.failure=type(error).__name__+': '+str(error)
        write(run.path/'business-failure.json',{'error':run.failure})
        print('BUSINESS_FAILURE '+run.failure,flush=True)
        raise
    finally:
        run.cleanup()
        print('BUSINESS_FINISHED '+str(run.path),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jmeter',type=Path,default=ROOT/'artifacts/test-runs/.tools/apache-jmeter-5.6.3/bin/jmeter.bat')
    parser.add_argument('--smoke',action='store_true')
    asyncio.run(main(parser.parse_args()))
