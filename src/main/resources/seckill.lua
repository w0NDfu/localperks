-- 1. Args
-- 1.1 voucher id
local voucherId = ARGV[1]
-- 1.2 user id
local userId = ARGV[2]

-- 2. Redis keys
local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId

-- The original two-argument callers keep their stock/qualification contract.
-- New HTTP callers also reserve one bounded outstanding request slot.
local orderId = ARGV[3]
-- Returning the existing qualification takes priority over capacity or sell-out.
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end
if orderId and redis.call('zcard', 'seckill:admission:pending') >= tonumber(ARGV[5]) then
    return 3
end

-- 3. Validate stock
local stock = tonumber(redis.call('get', stockKey))
if (stock == nil or stock <= 0) then
    return 1
end

-- 5. Reserve stock and user order qualification
redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
if orderId then
    local receipt = 'seckill:request:' .. orderId
    redis.call('hset', receipt, 'orderId', orderId, 'userId', userId, 'voucherId', voucherId, 'state', 'UNKNOWN', 'createdAt', ARGV[4])
    local userRequests = 'seckill:requests:user:' .. userId
    redis.call('zadd', userRequests, ARGV[4], orderId)
    redis.call('zremrangebyrank', userRequests, 0, -101)
    redis.call('expire', userRequests, 172800)
    redis.call('hset', 'seckill:request-user:' .. voucherId, userId, orderId)
    redis.call('zadd', 'seckill:admission:pending', ARGV[4], orderId)
    redis.call('zadd', 'seckill:admission:voucher:' .. voucherId, ARGV[4], orderId)
    redis.call('zadd', 'seckill:admission:unconfirmed', ARGV[4], orderId)
end
return 0
