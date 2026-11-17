package com.hmdp.mq;

import com.hmdp.ratelimit.RateLimitExceededException;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SeckillEligibilityReaderTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger orderStatus = new AtomicInteger(2);
    private final LocalDateTime begin = LocalDateTime.of(2026, 1, 1, 0, 0);

    private void stubQueries() {
        when(jdbc.query(anyString(), any(Object[].class), any(ResultSetExtractor.class))).thenAnswer(call -> {
            calls.incrementAndGet();
            Object[] arguments = call.getArgument(1);
            assertEquals(0, arguments.length % 3);
            assertTrue(call.<String>getArgument(0).contains("LEFT JOIN tb_voucher_order"));
            int count = arguments.length / 3;
            AtomicInteger slot = new AtomicInteger(count);
            ResultSet rows = mock(ResultSet.class);
            when(rows.next()).thenAnswer(ignored -> slot.decrementAndGet() >= 0);
            when(rows.getInt("slot")).thenAnswer(ignored -> slot.get());
            when(rows.getObject("order_id")).thenAnswer(ignored -> (long) arguments[slot.get()*3+1] % 2 == 1 ? 42L : null);
            when(rows.getLong("order_id")).thenReturn(42L);
            when(rows.getLong("user_id")).thenAnswer(ignored -> arguments[slot.get()*3+1]);
            when(rows.getLong("voucher_id")).thenAnswer(ignored -> arguments[slot.get()*3+2]);
            when(rows.getInt("status")).thenAnswer(ignored -> orderStatus.get());
            when(rows.getObject("begin_time", LocalDateTime.class)).thenReturn(begin);
            when(rows.getObject("end_time", LocalDateTime.class)).thenAnswer(ignored ->
                    Long.valueOf(999L).equals(arguments[slot.get()*3+2]) ? null : begin.plusDays(1));
            ResultSetExtractor<?> extractor = call.getArgument(2);
            return extractor.extractData(rows);
        });
    }

    @Test
    void shouldMapUnorderedRowsToTheirOwnUserAndVoucherWithoutDroppingMembers() throws Exception {
        stubQueries();
        SeckillEligibilityReader reader = new SeckillEligibilityReader(jdbc, true, 1, 32, 10, 64, 2000);
        ExecutorService clients = Executors.newFixedThreadPool(32);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<SeckillAdmissionRepository.Eligibility>> results = new ArrayList<>();
            for (long user = 1; user <= 32; user++) {
                final long uid = user;
                results.add(clients.submit(() -> { start.await(); return reader.inspect(uid, uid == 2 ? 999L : uid + 100); }));
            }
            start.countDown();
            for (int index = 0; index < results.size(); index++) {
                long user = index + 1;
                SeckillAdmissionRepository.Eligibility result = results.get(index).get(2, TimeUnit.SECONDS);
                assertEquals(begin, result.begin);
                if (user == 2) assertNull(result.end); else assertEquals(begin.plusDays(1), result.end);
                if (user % 2 == 0) assertNull(result.existing);
                else {
                    assertEquals(user, result.existing.getUserId());
                    assertEquals(user + 100, result.existing.getVoucherId());
                    assertEquals(2, result.existing.getStatus());
                }
            }
            assertTrue(calls.get() < 32);
        } finally { start.countDown(); clients.shutdownNow(); reader.close(); }
    }

    @Test
    void shouldNotCacheUserOrderStatusBetweenQueries() {
        stubQueries();
        SeckillEligibilityReader reader = new SeckillEligibilityReader(jdbc, true, 1, 1, 0, 1, 2000);
        try {
            assertEquals(2, reader.inspect(1L, 7L).existing.getStatus());
            orderStatus.set(4);
            assertEquals(4, reader.inspect(1L, 7L).existing.getStatus());
            assertEquals(2, calls.get());
        } finally { reader.close(); }
    }

    @Test
    void shouldPropagateDatabaseFailureAndKeepServingLaterRequests() {
        stubQueries();
        AtomicInteger attempts = new AtomicInteger();
        doAnswer(call -> {
            if (attempts.incrementAndGet() == 1) throw new DataAccessResourceFailureException("database unavailable");
            ResultSet rows = mock(ResultSet.class);
            when(rows.next()).thenReturn(true, false);
            when(rows.getInt("slot")).thenReturn(0);
            return call.<ResultSetExtractor<?>>getArgument(2).extractData(rows);
        }).when(jdbc).query(anyString(), any(Object[].class), any(ResultSetExtractor.class));
        SeckillEligibilityReader reader = new SeckillEligibilityReader(jdbc, true, 1, 1, 0, 1, 2000);
        try {
            assertThrows(DataAccessResourceFailureException.class, () -> reader.inspect(1L, 7L));
            assertNull(reader.inspect(2L, 7L).existing);
        } finally { reader.close(); }
    }

    @Test
    void shouldReturnBackpressureOnTimeoutBeforeAnyReservation() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        when(jdbc.query(anyString(), any(Object[].class), any(ResultSetExtractor.class))).thenAnswer(call -> {
            entered.countDown(); release.await(1, TimeUnit.SECONDS);
            ResultSet rows = mock(ResultSet.class);
            when(rows.next()).thenReturn(true, false);
            return call.<ResultSetExtractor<?>>getArgument(2).extractData(rows);
        });
        SeckillEligibilityReader reader = new SeckillEligibilityReader(jdbc, true, 1, 1, 0, 1, 30);
        try {
            assertThrows(RateLimitExceededException.class, () -> reader.inspect(1L, 7L));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
        } finally { release.countDown(); reader.close(); }
    }

    @Test
    void shouldRejectAfterShutdownWithoutReadingSql() {
        SeckillEligibilityReader reader = new SeckillEligibilityReader(jdbc, true, 1, 1, 0, 1, 2000);
        reader.close();
        assertThrows(RateLimitExceededException.class, () -> reader.inspect(1L, 7L));
        verifyNoInteractions(jdbc);
    }
}
