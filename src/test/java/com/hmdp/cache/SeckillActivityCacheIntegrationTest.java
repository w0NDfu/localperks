package com.hmdp.cache;

import com.hmdp.dto.OrderTimeoutMessage;
import com.hmdp.dto.VoucherOrderMessage;
import com.hmdp.mq.SeckillAdmissionState;
import com.hmdp.utils.RedisScripts;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named = "validation.mysql.port", matches = "[0-9]+")
class SeckillActivityCacheIntegrationTest {
    private static final AtomicLong IDS = new AtomicLong(850000);
    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private static JdbcTemplate jdbc;
    private static SeckillActivityCache cache;
    private static SeckillAdmissionState state;
    private long voucher;

    @BeforeAll
    static void connect() {
        factory = new LettuceConnectionFactory("127.0.0.1", Integer.parseInt(System.getProperty("validation.redis.port")));
        factory.afterPropertiesSet();
        redis = new StringRedisTemplate(factory);
        DriverManagerDataSource source = new DriverManagerDataSource("jdbc:mysql://127.0.0.1:"
                + System.getProperty("validation.mysql.port")
                + "/dianping_test?useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true",
                "root", System.getenv("VALIDATION_MYSQL_PASSWORD"));
        jdbc = new JdbcTemplate(source);
        state = new SeckillAdmissionState(redis);
        cache = new SeckillActivityCache(source, redis, state);
    }

    @AfterAll
    static void close() { factory.destroy(); }

    @BeforeEach
    void fixture() {
        voucher = IDS.incrementAndGet();
        jdbc.update("INSERT INTO tb_seckill_voucher(voucher_id,stock,begin_time,end_time) "
                + "VALUES(?,10,NOW()-INTERVAL 1 HOUR,NOW()+INTERVAL 1 HOUR)", voucher);
    }

    @AfterEach
    void clearPending() {
        // Exact IDs created by this test only; never flush an instance or erase another test's work.
        for (long user = 1; user <= 100; user++) {
            String order = Long.toString(order(user));
            redis.opsForZSet().remove(SeckillAdmissionState.PENDING, order);
            redis.opsForZSet().remove(SeckillAdmissionState.UNCONFIRMED, order);
        }
    }

    private long order(long user) { return voucher * 1000 + user; }

    private long reserve(long user) {
        return redis.execute(RedisScripts.load("seckill.lua", Long.class), Collections.emptyList(),
                Long.toString(voucher), Long.toString(user), Long.toString(order(user)),
                Long.toString(System.currentTimeMillis()), "2000");
    }

    private String stock() { return redis.opsForValue().get("seckill:stock:" + voucher); }

    @Test
    void shouldPrepareActivityBeforeAcceptingRequestsAndNeverResetLiveStock() {
        assertEquals(5, reserve(1));
        cache.warm(voucher);
        assertEquals(0, reserve(1));
        assertEquals("9", stock());
        cache.warm(voucher);
        assertEquals("9", stock());
        assertEquals(2, reserve(1));
    }

    @Test
    void shouldLoadHistoricalPurchasesIncludingCancelledOrdersWithoutOverwritingLiveInventory() {
        jdbc.update("INSERT INTO tb_voucher_order(id,user_id,voucher_id,status) VALUES(?,1,?,4)", order(1), voucher);
        redis.opsForValue().set("seckill:stock:" + voucher, "7");
        cache.warm(voucher);
        assertEquals(order(1), state.purchased(voucher, 1L).longValue());
        assertEquals(2, reserve(1));
        assertEquals("7", stock());
        assertEquals(0, reserve(2));
    }

    @Test
    void shouldNotRebuildMissingStockWhenHistoricalOrdersExist() {
        jdbc.update("INSERT INTO tb_voucher_order(id,user_id,voucher_id,status) VALUES(?,1,?,1)", order(1), voucher);
        cache.warm(voucher);
        assertNull(stock());
        assertEquals(5, reserve(2));
    }

    @Test
    void shouldFailClosedAfterLiveStockIsEvicted() {
        cache.warm(voucher);
        assertEquals(0, reserve(1));
        redis.delete("seckill:stock:" + voucher);
        cache.warm(voucher);
        assertNull(stock());
        assertEquals(5, reserve(2));
        state.releaseCancelled(new OrderTimeoutMessage(order(1), 1L, voucher));
        assertNull(stock());
    }

    @Test
    void shouldCheckActivityTimesInsideRedis() {
        jdbc.update("UPDATE tb_seckill_voucher SET begin_time=NOW()+INTERVAL 1 MINUTE,end_time=NOW()+INTERVAL 1 HOUR WHERE voucher_id=?", voucher);
        cache.warm(voucher);
        assertEquals(4, reserve(1));
        redis.opsForHash().put("seckill:activity:" + voucher, "beginAt", "1");
        redis.opsForHash().put("seckill:activity:" + voucher, "endAt", "2");
        assertEquals(4, reserve(1));
        assertEquals("10", stock());
    }

    @Test
    void shouldRejectPartialPurchaseStateUntilBackgroundPreheatCompletes() {
        cache.warm(voucher);
        jdbc.update("INSERT INTO tb_voucher_order(id,user_id,voucher_id,status) VALUES(?,1,?,1)", order(1), voucher);
        redis.delete("seckill:purchased:" + voucher);
        assertEquals(5, reserve(1));
        cache.warm(voucher);
        assertEquals(2, reserve(1));
        assertEquals("10", stock());
    }

    @Test
    void shouldPreservePurchaseQualificationAndReleaseCancelledStockOnlyOnce() {
        cache.warm(voucher);
        assertEquals(0, reserve(1));
        state.finish(new VoucherOrderMessage(order(1), 1L, voucher), "SUCCESS");
        OrderTimeoutMessage cancelled = new OrderTimeoutMessage(order(1), 1L, voucher);
        state.releaseCancelled(cancelled);
        state.releaseCancelled(cancelled);
        assertEquals("10", stock());
        assertEquals(2, reserve(1));
        assertEquals(order(1), state.purchased(voucher, 1L).longValue());
    }

    @Test
    void shouldNotReleaseACommittedPurchaseOnLateFailureCompensation() {
        cache.warm(voucher);
        assertEquals(0, reserve(1));
        state.finish(new VoucherOrderMessage(order(1), 1L, voucher), "SUCCESS");
        assertEquals(0L, redis.execute(RedisScripts.load("seckill_rollback.lua", Long.class),
                Collections.emptyList(), Long.toString(voucher), "1", Long.toString(order(1))));
        assertEquals("9", stock());
    }

    @Test
    void shouldAllowRetryAfterDefiniteCreationFailureButRejectOldCompensation() {
        cache.warm(voucher);
        assertEquals(0, reserve(1));
        redis.execute(RedisScripts.load("seckill_rollback.lua", Long.class), Collections.emptyList(),
                Long.toString(voucher), "1", Long.toString(order(1)));
        redis.execute(RedisScripts.load("seckill.lua", Long.class), Collections.emptyList(),
                Long.toString(voucher), "1", Long.toString(order(2)), Long.toString(System.currentTimeMillis()), "2000");
        assertEquals(0L, redis.execute(RedisScripts.load("seckill_rollback.lua", Long.class),
                Collections.emptyList(), Long.toString(voucher), "1", Long.toString(order(1))));
        assertEquals("9", stock());
    }

    @Test
    void shouldFenceStaleAndExpiredPreheatPublishers() {
        List<String> keys = Arrays.asList("seckill:activity:" + voucher, "seckill:stock:" + voucher,
                "seckill:purchased:" + voucher, "seckill:admission:voucher:" + voucher,
                "seckill:order:" + voucher, "seckill:request-user:" + voucher);
        assertEquals(1L, redis.execute(RedisScripts.load("seckill_preheat_claim.lua", Long.class), keys.subList(0, 3), "A"));
        redis.opsForHash().put(keys.get(0), "leaseUntil", "0");
        assertEquals(0L, redis.execute(RedisScripts.load("seckill_preheat_publish.lua", Long.class), keys,
                "A", "1", "9999999999999", "10", "1"));
        assertEquals(1L, redis.execute(RedisScripts.load("seckill_preheat_claim.lua", Long.class), keys.subList(0, 3), "B"));
        assertEquals(0L, redis.execute(RedisScripts.load("seckill_preheat_publish.lua", Long.class), keys,
                "A", "1", "9999999999999", "10", "1"));
        assertNull(stock());
    }

    @Test
    void shouldNotOversellWithConcurrentRedisAdmission() throws Exception {
        cache.warm(voucher);
        ExecutorService pool = Executors.newFixedThreadPool(20);
        List<Future<Long>> results = new ArrayList<>();
        try {
            for (long user = 1; user <= 100; user++) {
                final long id = user;
                results.add(pool.submit(() -> reserve(id)));
            }
            int accepted = 0;
            for (Future<Long> result : results) if (result.get(5, TimeUnit.SECONDS) == 0) accepted++;
            assertEquals(10, accepted);
            assertEquals("0", stock());
        } finally { pool.shutdownNow(); }
    }
}
