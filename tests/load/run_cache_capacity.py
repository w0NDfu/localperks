"""Measure cached shop HTTP throughput under a frozen P99<=100ms gate, in owned middleware."""
import argparse
import asyncio
import hashlib
import json
import shutil
import threading
import time
from pathlib import Path
import httpx
from redis.asyncio import Redis
from run_http_perf import HttpPerfRun, ROOT, HERE, patch_source, write


class CacheRun(HttpPerfRun):
    def prepare_source(self, work):
        super().prepare_source(work)
        java = work / 'src/main/java/com/hmdp'
        shutil.copy2(HERE / 'CacheReadProbe.java', java / 'perf/CacheReadProbe.java')
        patch_source(java / 'cache/ShopCacheService.java', [
            ('        if (!bloomFilterService.mightContain(id)) {',
             '        com.hmdp.perf.CacheReadProbe.requests.increment();\n        if (!bloomFilterService.mightContain(id)) {'),
            ('        if (localShop != null) {\n            return localShop;',
             '        if (localShop != null) {\n            com.hmdp.perf.CacheReadProbe.l1Hits.increment();\n            return localShop;'),
            ('        Shop shop = cacheClient.queryWithPassThrough(',
             '        com.hmdp.perf.CacheReadProbe.l2Lookups.increment();\n        Shop shop = cacheClient.queryWithPassThrough('),
            ('CACHE_SHOP_KEY, id, Shop.class, dbFallback, CACHE_SHOP_TTL, TimeUnit.MINUTES);',
             'CACHE_SHOP_KEY, id, Shop.class, item -> { com.hmdp.perf.CacheReadProbe.databaseFallbacks.increment(); return dbFallback.apply(item); }, CACHE_SHOP_TTL, TimeUnit.MINUTES);'),
        ])
        patch_source(java / 'cache/ShopBloomFilterService.java', [
            ('        if (!ready || filter == null) {\n            return true;',
             '        if (!ready || filter == null) {\n            com.hmdp.perf.CacheReadProbe.bloomBypass.increment();\n            return true;'),
            ('            log.error("查询店铺布隆过滤器失败，将降级放行，shopId={}", shopId, e);',
             '            com.hmdp.perf.CacheReadProbe.bloomErrors.increment();\n            log.error("查询店铺布隆过滤器失败，将降级放行，shopId={}", shopId, e);'),
        ])


async def counters(run, minimum=None):
    for _ in range(30):
        path = run.path / 'cache-counters.jsonl'
        if path.exists():
            lines = path.read_text(encoding='utf-8').splitlines()
            if lines:
                row = json.loads(lines[-1])
                if minimum is None or row['requests'] >= minimum:
                    return row
        await asyncio.sleep(.2)
    raise RuntimeError('Cache counter snapshot unavailable/incomplete')


def passes_cache_gate(row):
    """Validate observed work; nominal duration*rate is not an exact scheduler count.

    In particular, extra boundary arrivals are real verified work, not missing
    requests. Keep zero dropped iterations and a minimum actual delivery rate.
    """
    delta = row['cache_counters']
    return (row['completed'] == row['valid'] and row['completed'] > 0
            and row['actual_iteration_rps'] >= row['target_rps'] * .99
            and row['invalid'] == row['dropped'] == 0
            and row['http_failed_rate'] == 0 and row['e2e_p99_ms'] <= 100 and row['k6_exit'] == 0
            and delta['requests'] == row['completed']
            and delta['databaseFallbacks'] == delta['bloomBypass'] == delta['bloomErrors'] == 0
            and row['l1_hit_ratio'] >= .99)


async def tier(run, k6, rate, seconds, tag):
    run.stage(tag)
    run.sample('before-' + tag, guard=True)
    before = await counters(run)
    fixture = run.path / 'temp' / (tag + '.json')
    write(fixture, {'baseUrl': run.java_url, 'rate': rate, 'seconds': seconds, 'vus': run.cache_vus,
                    'ids': [9001, 9002, 9003], 'names': {str(9000+i): f'验收餐厅{i}' for i in range(1, 4)}})
    summary_path = run.path / (tag + '-k6.json')
    env = dict(run.env, SHOPLINK_CACHE_FIXTURE=str(fixture), SHOPLINK_CACHE_SUMMARY=str(summary_path),
               K6_NO_USAGE_REPORT='true', NO_PROXY='127.0.0.1')
    started = int(time.time()*1000)
    result = await asyncio.to_thread(run.command, [k6, 'run', '--quiet', '--no-color', HERE / 'cache_reads.k6.js'],
                                    timeout=seconds+35, env=env, check=False)
    ended = int(time.time()*1000)
    (run.path / (tag + '-console.log')).write_text(result.stdout + result.stderr, encoding='utf-8')
    summary = json.loads(summary_path.read_text(encoding='utf-8'))
    metrics = summary['metrics']
    def value(name, key='count', default=0):
        return metrics.get(name, {}).get('values', {}).get(key, default)
    completed = int(value('iterations'))
    expected = rate*seconds
    after = await counters(run, before['requests'] + int(value('http_reqs')) - int(value('http_req_failed', 'passes')))
    delta = {key: after[key]-before[key] for key in ('requests','l1Hits','l2Lookups','databaseFallbacks','bloomBypass','bloomErrors')}
    row = {'tag': tag, 'target_rps': rate, 'seconds': seconds, 'expected': expected, 'completed': completed,
           'valid': int(value('cache_valid_responses')), 'invalid': int(value('cache_invalid_responses')),
           'dropped': int(value('dropped_iterations')), 'http_failed_rate': value('http_req_failed', 'rate'),
           'actual_iteration_rps': value('iterations', 'rate'),
           'e2e_p50_ms': value('cache_http_e2e_ms','med'), 'e2e_p95_ms': value('cache_http_e2e_ms','p(95)'),
           'e2e_p99_ms': value('cache_http_e2e_ms','p(99)'), 'e2e_max_ms': value('cache_http_e2e_ms','max'),
           'http_duration_p99_ms': value('http_req_duration','p(99)'), 'k6_exit': result.returncode,
           'cache_counters': delta, 'phase_started_ms': started, 'phase_ended_ms': ended,
           'l1_hit_ratio': delta['l1Hits']/max(1,delta['requests'])}
    row['arrival_boundary_delta'] = completed - expected
    row['pass'] = passes_cache_gate(row)
    write(run.path / (tag + '-audit.json'), {'result': row, 'k6_state': summary.get('state'), 'before': before, 'after': after})
    print(json.dumps(row), flush=True)
    await asyncio.sleep(2)
    return row


async def main(args):
    run = CacheRun(100, 'cache-p99-100ms')
    run.cache_vus = args.vus
    run.storage_profile = 'disk'
    run.env['SHOPLINK_CACHE_METRICS'] = str(run.path / 'cache-counters.jsonl')
    run.plan = HERE / 'cache_reads.k6.js'
    write(run.path / 'frozen-cache-gate.json', {'endpoint': 'GET /shop/{id}', 'ids': [9001,9002,9003],
        'profile': 'Warm 3-shop working set; complete public HTTP path, remote Redisson Bloom before L1, default L1 TTL60s',
        'gate': {'p99_ms':100,'errors':0,'dropped_iterations':0,'database_fallbacks_after_warmup':0,'l1_hit_ratio_min':.99},
        'rates': args.rates, 'step_seconds':args.step_seconds,'steady_seconds':args.steady_seconds,
        'refine_rounds':2,'fixed_k6_vus':args.vus,'shop_endpoint_limits_unchanged':True,
        'nominal_arrival_count_is_diagnostic_only':True,'minimum_actual_target_rate_ratio':.99,
        'missing_work_gate':'k6 dropped_iterations must be zero; completed must equal valid and observed endpoint count',
        'source_sha256':{str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in (
            HERE/'run_cache_capacity.py',HERE/'cache_reads.k6.js',HERE/'CacheReadProbe.java',
            ROOT/'src/main/java/com/hmdp/cache/ShopCacheService.java',
            ROOT/'src/main/java/com/hmdp/cache/ShopBloomFilterService.java')}})
    print('CACHE_CAPACITY ' + str(run.path), flush=True)
    rows = []
    try:
        run.sample('before', guard=True)
        run.build()
        await run.infrastructure()
        run.monitor_thread=threading.Thread(target=run.monitor,daemon=True);run.monitor_thread.start()
        async with httpx.AsyncClient(base_url=run.java_url,timeout=5,trust_env=False) as client:
            for _ in range(2):
                for shop in (9001,9002,9003):
                    response=await client.get('/shop/'+str(shop)); response.raise_for_status()
                    assert response.json()['success'] and response.json()['data']['id']==shop
        await asyncio.sleep(2)
        warm = await counters(run, 6)
        assert warm['databaseFallbacks']==3 and warm['bloomBypass']==warm['bloomErrors']==0, warm
        write(run.path / 'warmup.json', warm)
        async with Redis.from_url(run.redis_url,decode_responses=True) as redis:
            write(run.path / 'redis-commandstats-before.json', await redis.info('commandstats'))
        k6=args.k6.resolve(strict=True)
        low, high = 0, None
        for rate in args.rates:
            row=await tier(run,k6,rate,args.step_seconds,'step-'+str(rate));rows.append(row)
            write(run.path / 'cache-results.json',rows)
            if row['pass']:low=rate
            else:high=rate;break
        if low and high:
            for _ in range(2):
                rate=(low+high)//100*50
                if rate<=low or rate>=high:break
                row=await tier(run,k6,rate,args.step_seconds,'refine-'+str(rate));rows.append(row)
                write(run.path / 'cache-results.json',rows)
                if row['pass']:low=rate
                else:high=rate
        candidates=sorted({r['target_rps'] for r in rows if r['pass']},reverse=True)
        validated=None
        for rate in candidates:
            row=await tier(run,k6,rate,args.steady_seconds,'steady-'+str(rate));rows.append(row)
            write(run.path / 'cache-results.json',rows)
            if row['pass']:validated=row;break
        async with Redis.from_url(run.redis_url,decode_responses=True) as redis:
            write(run.path / 'redis-commandstats-after.json',await redis.info('commandstats'))
        final={'run':run.id,'validated_steady':validated,'first_failed_step':high,'rows':rows,
               'scope':'Single host, warm three-shop HTTP cache reads; not raw Redis/Caffeine capacity or production maximum'}
        write(run.path / 'cache-capacity-summary.json',final)
        print('CACHE_FINAL '+json.dumps({key: final[key] for key in ('run','validated_steady','first_failed_step')}),flush=True)
        if validated is None:raise RuntimeError('No sustained tier passed P99<=100ms')
    except BaseException as error:
        write(run.path/'cache-harness-failure.json',{'error':type(error).__name__,'detail':str(error),'completed_tiers':rows})
        raise
    finally:
        run.cleanup()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--k6',type=Path,required=True)
    parser.add_argument('--rates',type=lambda s:list(map(int,s.split(','))),default=[100,500,1000,2000,4000,8000])
    parser.add_argument('--step-seconds',type=int,default=45)
    parser.add_argument('--steady-seconds',type=int,default=180)
    parser.add_argument('--vus',type=int,default=256)
    asyncio.run(main(parser.parse_args()))
