package com.hmdp.mq;

import com.hmdp.dto.VoucherOrderMessage;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import com.hmdp.dto.VoucherWalletEntry;

/** Receipts recover uncertain sends; Broker confirmation is required for new acceptance. */
@Component
public class SeckillAdmissionState {
    public static final String PENDING = "seckill:admission:pending";
    public static final String UNCONFIRMED = "seckill:admission:unconfirmed";
    private static final DefaultRedisScript<Long> CONFIRM = new DefaultRedisScript<>(
            "if redis.call('hget',KEYS[1],'state')=='UNKNOWN' then redis.call('hset',KEYS[1],'state','ACCEPTED') end "
                    + "redis.call('zrem',KEYS[2],ARGV[1]) return 1", Long.class);
    private static final DefaultRedisScript<Long> FINISH = new DefaultRedisScript<>(
            "if redis.call('exists',KEYS[1])==1 then redis.call('hset',KEYS[1],'state',ARGV[2]) redis.call('expire',KEYS[1],86400) end "
                    + "redis.call('zrem',KEYS[2],ARGV[1]) redis.call('zrem',KEYS[3],ARGV[1]) "
                    + "redis.call('zrem',KEYS[4],ARGV[1]) return 1", Long.class);
    private static final DefaultRedisScript<Long> DEFER = new DefaultRedisScript<>(
            "if redis.call('zscore',KEYS[1],ARGV[1]) then redis.call('zadd',KEYS[1],ARGV[2],ARGV[1]) end return 1", Long.class);
    private static final DefaultRedisScript<Long> CLAIM_SEND = new DefaultRedisScript<>(
            "if redis.call('hget',KEYS[1],'state')~='UNKNOWN' then return 0 end "
                    + "if not redis.call('zscore',KEYS[2],ARGV[1]) then return 0 end "
                    + "if not redis.call('set',KEYS[3],ARGV[2],'NX','PX',10000) then return 0 end "
                    + "redis.call('zadd',KEYS[2],ARGV[3],ARGV[1]) return 1", Long.class);
    private static final DefaultRedisScript<Long> RELEASE_SEND = new DefaultRedisScript<>(
            "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) end return 0", Long.class);
    private final StringRedisTemplate redis;

    public SeckillAdmissionState(StringRedisTemplate redis) {
        this.redis = redis;
    }

    public static String key(Long orderId) {
        return "seckill:request:" + orderId;
    }

    public Long previous(Long voucherId, Long userId) {
        Object value = redis.opsForHash().get("seckill:request-user:" + voucherId, userId.toString());
        return value == null ? null : Long.valueOf(value.toString());
    }

    public Map<Object, Object> receipt(Long orderId, Long userId) {
        Map<Object, Object> row = redis.opsForHash().entries(key(orderId));
        return userId.toString().equals(row.get("userId")) ? row : Collections.emptyMap();
    }

    public void accepted(Long orderId) {
        redis.execute(CONFIRM, Arrays.asList(key(orderId), UNCONFIRMED), orderId.toString());
    }

    public boolean claimSend(Long orderId, String token) {
        return Long.valueOf(1).equals(redis.execute(CLAIM_SEND,
                Arrays.asList(key(orderId), UNCONFIRMED, key(orderId) + ":send-lock"),
                orderId.toString(), token, Long.toString(System.currentTimeMillis())));
    }

    public void releaseSend(Long orderId, String token) {
        redis.execute(RELEASE_SEND, Collections.singletonList(key(orderId) + ":send-lock"), token);
    }

    /** Bounded per-user recent receipts, joined with SQL history without storing admission rows. */
    public List<VoucherWalletEntry> recent(Long userId) {
        List<VoucherWalletEntry> result = new ArrayList<>();
        Set<String> ids = redis.opsForZSet().reverseRange("seckill:requests:user:" + userId, 0, 99);
        if (ids == null) return result;
        for (String id : ids) {
            Map<Object, Object> row = receipt(Long.valueOf(id), userId);
            if (row.isEmpty()) continue;
            VoucherWalletEntry entry = new VoucherWalletEntry();
            entry.setOrderId(Long.valueOf(id));
            entry.setVoucherId(Long.valueOf(row.get("voucherId").toString()));
            entry.setMessageStatus("UNKNOWN".equals(row.get("state")) ? 19
                    : "FAILED".equals(row.get("state")) ? 2 : 1);
            Object stamp = row.get("createdAt");
            entry.setCreateTime(stamp == null ? LocalDateTime.now()
                    : LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(stamp.toString())), ZoneId.systemDefault()));
            result.add(entry);
        }
        return result;
    }

    public void finish(VoucherOrderMessage message, String state) {
        redis.execute(FINISH, Arrays.asList(key(message.getOrderId()), PENDING, UNCONFIRMED,
                        "seckill:admission:voucher:" + message.getVoucherId()),
                message.getOrderId().toString(), state);
    }

    public Set<String> due(boolean unconfirmed) {
        return redis.opsForZSet().rangeByScore(unconfirmed ? UNCONFIRMED : PENDING,
                0, System.currentTimeMillis() - (unconfirmed ? 5000 : 30000), 0, 32);
    }

    public VoucherOrderMessage message(String orderId) {
        Map<Object, Object> row = redis.opsForHash().entries(key(Long.valueOf(orderId)));
        if (row.isEmpty()) {
            throw new IllegalStateException("Unresolved reservation has no receipt: " + orderId);
        }
        return new VoucherOrderMessage(Long.valueOf(orderId), Long.valueOf(row.get("userId").toString()),
                Long.valueOf(row.get("voucherId").toString()));
    }

    public void defer(Long orderId) {
        redis.execute(DEFER, Collections.singletonList(PENDING), orderId.toString(),
                Long.toString(System.currentTimeMillis()));
    }
}
