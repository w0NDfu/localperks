-- Only terminal consumer failure / cancellation may compensate; an uncertain send must not.
local voucherId = ARGV[1]
local userId = ARGV[2]

local stockKey = 'seckill:stock:' .. voucherId
local orderKey = 'seckill:order:' .. voucherId
if ARGV[3] then
    local owner = redis.call('hget', 'seckill:request-user:' .. voucherId, userId)
    if owner and owner ~= ARGV[3] then return 0 end
end

if redis.call('srem', orderKey, userId) == 1 then
    redis.call('incrby', stockKey, 1)
end
return 0
