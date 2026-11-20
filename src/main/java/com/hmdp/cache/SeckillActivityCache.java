package com.hmdp.cache;

import com.hmdp.dto.OrderTimeoutMessage;
import com.hmdp.mq.SeckillAdmissionState;
import com.hmdp.utils.RedisScripts;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Prepare admission state outside HTTP. SQL remains authoritative for committed orders. */
@Slf4j
@Component
public class SeckillActivityCache {
    private static final DefaultRedisScript<Long> CLAIM = RedisScripts.load("seckill_preheat_claim.lua", Long.class);
    private static final DefaultRedisScript<Long> PUBLISH = RedisScripts.load("seckill_preheat_publish.lua", Long.class);
    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final SeckillAdmissionState admission;
    private long cancellationCursor;

    public SeckillActivityCache(DataSource source, StringRedisTemplate redis, SeckillAdmissionState admission) {
        this.jdbc = new JdbcTemplate(source);
        this.redis = redis;
        this.admission = admission;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initialize() {
        refresh();
    }

    @Scheduled(initialDelayString = "${hmdp.seckill.preheat-fixed-delay:60000}",
            fixedDelayString = "${hmdp.seckill.preheat-fixed-delay:60000}")
    public synchronized void refresh() {
        try {
            long cursor = 0;
            List<Long> ids;
            do {
                ids = jdbc.queryForList("SELECT voucher_id FROM tb_seckill_voucher "
                        + "WHERE voucher_id>? AND end_time>NOW() ORDER BY voucher_id LIMIT 100", Long.class, cursor);
                for (Long id : ids) {
                    try { warm(id); }
                    catch (Exception error) { log.warn("Seckill activity remains unavailable, voucherId={}", id, error); }
                    cursor = id;
                }
            } while (ids.size() == 100);
            reconcileCancellations();
        } catch (Exception error) {
            log.warn("Seckill background preheat/reconciliation will retry", error);
        }
    }

    public void warm(Long voucherId) {
        String activity = "seckill:activity:" + voucherId;
        String stock = "seckill:stock:" + voucherId;
        String purchased = "seckill:purchased:" + voucherId;
        String token = UUID.randomUUID().toString();
        Long claimed = redis.execute(CLAIM, Arrays.asList(activity, stock, purchased), token);
        if (Long.valueOf(-1).equals(claimed)) {
            log.error("Live Redis stock missing; admission stays closed, reconcile offline, voucherId={}", voucherId);
            return;
        }
        if (!Long.valueOf(1).equals(claimed)) return;
        List<long[]> activities = jdbc.query("SELECT stock,UNIX_TIMESTAMP(begin_time)*1000,"
                + "UNIX_TIMESTAMP(end_time)*1000 FROM tb_seckill_voucher WHERE voucher_id=?",
                (row, index) -> new long[]{row.getLong(1), row.getLong(2), row.getLong(3)}, voucherId);
        if (activities.isEmpty()) return;
        long[] row = activities.get(0);
        if (row[0] < 0 || row[1] >= row[2]) throw new IllegalStateException("Invalid seckill activity");
        long cursor = 0;
        boolean virgin = true;
        List<long[]> orders;
        do {
            orders = jdbc.query("SELECT id,user_id FROM tb_voucher_order WHERE voucher_id=? AND id>? "
                    + "ORDER BY id LIMIT 500", (order, index) -> new long[]{order.getLong(1), order.getLong(2)},
                    voucherId, cursor);
            Map<String, String> history = new LinkedHashMap<>();
            for (long[] order : orders) {
                history.put(Long.toString(order[1]), Long.toString(order[0]));
                cursor = order[0];
                virgin = false;
            }
            // Additive history, including cancelled orders; never clear concurrent committed purchases.
            if (!history.isEmpty()) redis.opsForHash().putAll(purchased, history);
        } while (orders.size() == 500);
        Long published = redis.execute(PUBLISH, Arrays.asList(activity, stock, purchased,
                        "seckill:admission:voucher:" + voucherId, "seckill:order:" + voucherId,
                        "seckill:request-user:" + voucherId), token, Long.toString(row[1]), Long.toString(row[2]),
                Long.toString(row[0]), virgin ? "1" : "0");
        if (!Long.valueOf(1).equals(published)) {
            log.warn("Seckill preheat not published; keep admission closed, voucherId={}, result={}", voucherId, published);
        }
    }

    /** Retry an after-commit release lost to a crash/Redis outage; the Lua release is idempotent. */
    private void reconcileCancellations() {
        List<OrderTimeoutMessage> cancelled = jdbc.query("SELECT id,user_id,voucher_id "
                + "FROM tb_voucher_order WHERE id>? AND status=4 ORDER BY id LIMIT 100",
                (row, index) -> new OrderTimeoutMessage(row.getLong(1), row.getLong(2), row.getLong(3)), cancellationCursor);
        for (OrderTimeoutMessage order : cancelled) {
            admission.releaseCancelled(order);
            cancellationCursor = order.getOrderId();
        }
        if (cancelled.size() < 100) cancellationCursor = 0;
    }
}
