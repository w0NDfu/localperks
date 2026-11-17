"""Apply timing-only instrumentation to the isolated build."""
import shutil
from pathlib import Path

from run_perf import patch_source


def instrument(java):
    here = Path(__file__).resolve().parent
    for source in (here / 'precise').glob('*.java'):
        shutil.copy2(source, java / 'perf' / source.name)
    for relative in ('service/impl/VoucherOrderServiceImpl.java', 'mq/SeckillOrderMessageService.java',
                     'mq/SeckillAdmissionState.java', 'ratelimit/SlidingWindowRateLimiter.java', 'utils/RedisScripts.java'):
        path = java / relative
        if path.exists():
            text = path.read_text(encoding='utf-8').replace(
                'new DefaultRedisScript<', 'new com.hmdp.perf.TimedRedisScript<')
            path.write_text(text, encoding='utf-8')
    patch_source(java / 'controller/VoucherOrderController.java', [
        ('        return voucherOrderService.seckillVoucher(voucherId);',
         '        long controllerStarted = System.nanoTime();\n        try { return voucherOrderService.seckillVoucher(voucherId); }\n        finally { com.hmdp.perf.PerfProbe.returned(controllerStarted); }'),
    ])
    patch_source(java / 'service/impl/VoucherOrderServiceImpl.java', [
        ('long orderId = redisIdWorker.nextId("order");',
         'long orderId = redisIdWorker.nextId("order");\n            com.hmdp.perf.PerfProbe.order(orderId);'),
        ('        long luaStarted = System.nanoTime();',
         '        long luaStarted = System.nanoTime();\n        com.hmdp.perf.PerfProbe.phase("reserve");'),
        ('        com.hmdp.perf.PerfProbe.duration("reserve_lua_rtt", luaStarted);',
         '        com.hmdp.perf.PerfProbe.duration("reserve_lua_rtt", luaStarted);\n        com.hmdp.perf.PerfProbe.phase("");'),
    ])
    patch_source(java / 'ratelimit/SlidingWindowRateLimiter.java', [
        ('        long started = System.nanoTime();',
         '        long started = System.nanoTime();\n        com.hmdp.perf.PerfProbe.phase("rate");'),
        ('        com.hmdp.perf.PerfProbe.duration("rate_lua_rtt", started);',
         '        com.hmdp.perf.PerfProbe.duration("rate_lua_rtt", started);\n        com.hmdp.perf.PerfProbe.phase("");'),
    ])
    patch_source(java / 'utils/RefreshTokenInterceptor.java', [
        ('public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {',
         'public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {\n        long identityTotal = System.nanoTime();\n        com.hmdp.perf.PerfProbe.phase("identity");\n        try {'),
        ('\n    }\n\n    @Override\n    public void afterCompletion',
         '\n        } finally { com.hmdp.perf.PerfProbe.duration("identity_total", identityTotal); com.hmdp.perf.PerfProbe.phase(""); }\n    }\n\n    @Override\n    public void afterCompletion'),
    ])
    publisher = java / 'mq/SeckillOrderMessageService.java'
    text = publisher.read_text(encoding='utf-8')
    if 'private SendResult sendConfirmed' in text:
        text = text.replace('com.hmdp.perf.PerfProbe.duration("seckill_producer_ack", publishStarted);',
            'com.hmdp.perf.PerfProbe.durationFor("seckill_producer_ack", publishStarted, message.getUserId(), message.getVoucherId(), message.getOrderId());')
    modern = 'SeckillAdmissionRepository admission' in text
    row = 'row' if modern else 'mqMessage'
    timings = () if 'private SendResult sendConfirmed' in text else (
        ('seckill_producer_ack', 'publishStarted'), ('seckill_ack_sql', 'callbackStarted'),
        ('seckill_producer_failed', 'publishStarted'))
    for name, started in timings:
        before = f'com.hmdp.perf.PerfProbe.duration("{name}", {started});'
        after = (f'com.hmdp.perf.PerfProbe.durationFor("{name}", {started}, '
                 f'{row}.getUserId(), {row}.getVoucherId(), {row}.getOrderId());')
        if text.count(before) != 1:
            raise ValueError('Producer timing anchor changed: ' + name)
        text = text.replace(before, after)
    publisher.write_text(text, encoding='utf-8')
    observer = java / 'ops/OpsReadService.java'
    patch_source(observer, [
        ('                    ConsumeStats stats = api.getConsumeStats(',
         '''                    java.util.Map<String,String> rawRuntime = api.getBrokerRuntimeInfo(address, 1500).getTable();
                    java.util.Map<String,String> selectedRuntime = new java.util.LinkedHashMap<>();
                    for (String name : new String[]{"putTps","putMessageEntireTimeMax","putMessageDistributeTime",
                            "sendThreadPoolQueueSize","sendThreadPoolQueueHeadWaitTimeMills","pageCacheLockTimeMills","dispatchBehindBytes"}) {
                        if (rawRuntime.containsKey(name)) selectedRuntime.put(name, rawRuntime.get(name));
                    }
                    out.put("brokerRuntime", selectedRuntime);
                    ConsumeStats stats = api.getConsumeStats('''),
    ])
