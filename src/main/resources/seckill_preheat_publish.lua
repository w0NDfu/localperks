-- Publish only the current warmup generation; an expired owner cannot publish.
if redis.call('hget', KEYS[1], 'token') ~= ARGV[1] then return 0 end
local clock = redis.call('time')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if tonumber(redis.call('hget', KEYS[1], 'leaseUntil') or '0') <= now then return 0 end
if redis.call('exists', KEYS[2]) == 0 then
    -- Only a virgin activity may initialize inventory. Historical/pending orders need reconciliation.
    if ARGV[5] ~= '1' or redis.call('hget', KEYS[1], 'initialized') == '1'
        or redis.call('zcard', KEYS[4]) > 0 or redis.call('scard', KEYS[5]) > 0
        or redis.call('hlen', KEYS[6]) > 0 then return -1 end
    redis.call('set', KEYS[2], ARGV[4])
end
redis.call('hset', KEYS[3], '__ready__', '1')
redis.call('hset', KEYS[1], 'beginAt', ARGV[2], 'endAt', ARGV[3], 'ready', '1', 'initialized', '1')
redis.call('hdel', KEYS[1], 'token', 'leaseUntil')
return 1
