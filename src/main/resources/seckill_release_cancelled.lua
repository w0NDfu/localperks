-- Called only after SQL cancellation has committed, also retried by the background scanner.
-- Keep the permanent purchase marker: a cancelled voucher cannot be purchased a second time.
local voucherId, userId, orderId = ARGV[1], ARGV[2], ARGV[3]
local owner = redis.call('hget', 'seckill:request-user:' .. voucherId, userId)
if owner ~= orderId then return 0 end
local stockKey = 'seckill:stock:' .. voucherId
if redis.call('exists', stockKey) == 0 then return 0 end
redis.call('hsetnx', 'seckill:purchased:' .. voucherId, userId, orderId)
if redis.call('srem', 'seckill:order:' .. voucherId, userId) == 1 then
    redis.call('incrby', stockKey, 1)
    return 1
end
return 0
