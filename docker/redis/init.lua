-- Initialize only a fresh volume. Never reset stock or purchase records on restart.
if redis.call('EXISTS', 'localperks:docker:initialized:v1') == 1 then
    return 1
end

redis.call('SET', 'seckill:stock:1', '100', 'NX')
redis.call('GEOADD', 'shop:geo:1', 'NX', 120.149192, 30.316078, '1')
redis.call('SET', 'localperks:docker:initialized:v1', '1')
return 1
