// Precompile this same script in setup, then warm the injector before POST arrivals.
long now = System.currentTimeMillis()
long start = Long.parseLong(props.get('mainEpoch') ?: '0') + 10000
long finish = start + Long.parseLong(props.get('seconds')) * 1000
String bucket = ctx.getThreadGroup().getName().startsWith('Warm') ? 'warmup' :
    now < start ? 'ramp' : now < finish ? 'steady' : 'late'
vars.put('bucket', bucket)
vars.put('orderId', '')
vars.put('outcome', '')
boolean post = bucket in ['warmup', 'steady']
vars.put('request_method', post ? 'POST' : 'GET')
vars.put('request_path', !post ? '/shop-type/list' : '/voucher-order/seckill/' +
    (bucket == 'warmup' ? props.get('warmVoucher') : props.get('voucher')))
if (ctx.getThreadGroup().numberOfActiveThreads() > 2048) {
    props.put('injectorCapHit', 'true')
    new File(props.get('capFile')).text = Long.toString(now)
    ctx.getEngine().stopTest(true)
}
