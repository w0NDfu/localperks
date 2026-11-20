-- Only terminal consumer failure / cancellation may compensate; an uncertain send must not.
local voucherId = ARGV[1]
local userId = ARGV[2]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
-- A committed purchase must never be undone by a late failure compensation.
if redis.call('hexists', 'seckill:purchased:' .. voucherId, userId) == 1 then return 0 end
if redis.call('exists', stockKey) == 0 then return 0 end
if ARGV[3] then
    local owner = redis.call('hget', 'seckill:request-user:' .. voucherId, userId)
    if owner and owner ~= ARGV[3] then return 0 end
end

if redis.call('srem', orderKey, userId) == 1 then
    redis.call('incrby', stockKey, 1)
end
return 0
