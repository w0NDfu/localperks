-- Repeated startup must not overwrite an existing live inventory or purchase history.
if redis.call('hget', KEYS[1], 'ready') == '1'
    and redis.call('exists', KEYS[2]) == 1
    and redis.call('hget', KEYS[3], '__ready__') == '1' then return 0 end
-- Loss of live stock requires offline reconciliation, never refill from a SQL snapshot.
if redis.call('exists', KEYS[2]) == 0 and
    (redis.call('hget', KEYS[1], 'initialized') == '1'
    or redis.call('hget', KEYS[3], '__ready__') == '1') then return -1 end
local clock = redis.call('time')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
if tonumber(redis.call('hget', KEYS[1], 'leaseUntil') or '0') > now then return 0 end
redis.call('hset', KEYS[1], 'ready', '0', 'token', ARGV[1], 'leaseUntil', now + 30000)
return 1
