package com.hmdp.mq;

import com.hmdp.entity.VoucherOrder;
import com.hmdp.ratelimit.RateLimitExceededException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Coalesce fresh indexed reads, never cache a user's eligibility or skip legacy-order checks. */
@Component
public class SeckillEligibilityReader {
    private static final String COLUMNS = "o.id AS order_id,o.user_id,o.voucher_id,o.status,o.pay_time,o.create_time,s.begin_time,s.end_time ";
    private static final String JOINS = " LEFT JOIN tb_seckill_voucher s ON s.voucher_id=r.vid"
            + " LEFT JOIN tb_voucher_order o ON o.user_id=r.uid AND o.voucher_id=r.vid";
    private final JdbcTemplate jdbc;
    private final boolean enabled;
    private final int size;
    private final long flushNanos;
    private final long waitMillis;
    private final ArrayBlockingQueue<Pending> queue;
    private final List<Thread> workers = new ArrayList<>();
    private final Object lifecycle = new Object();
    private volatile boolean closed;

    private static class Pending {
        final Long user, voucher;
        final CompletableFuture<SeckillAdmissionRepository.Eligibility> result = new CompletableFuture<>();
        Pending(Long user, Long voucher) { this.user = user; this.voucher = voucher; }
    }

    @Autowired
    public SeckillEligibilityReader(@Qualifier("seckillAdmissionDataSource") DataSource source,
            @Value("${hmdp.seckill.eligibility-batch.enabled:true}") boolean enabled,
            @Value("${hmdp.seckill.eligibility-batch.workers:2}") int workerCount,
            @Value("${hmdp.seckill.eligibility-batch.size:32}") int size,
            @Value("${hmdp.seckill.eligibility-batch.flush-millis:1}") long flushMillis,
            @Value("${hmdp.seckill.eligibility-batch.capacity:1024}") int capacity,
            @Value("${hmdp.seckill.eligibility-batch.wait-millis:2000}") long waitMillis) {
        this(new JdbcTemplate(source), enabled, workerCount, size, flushMillis, capacity, waitMillis);
    }

    SeckillEligibilityReader(JdbcTemplate jdbc, boolean enabled, int workerCount, int size,
                            long flushMillis, int capacity, long waitMillis) {
        if (workerCount < 1 || workerCount > 4 || size < 1 || size > 32 || flushMillis < 0
                || flushMillis > 10 || capacity < size || capacity > 4096 || waitMillis < 1)
            throw new IllegalArgumentException("Invalid eligibility read budget");
        this.jdbc = jdbc;
        this.enabled = enabled;
        this.size = size;
        this.flushNanos = TimeUnit.MILLISECONDS.toNanos(flushMillis);
        this.waitMillis = waitMillis;
        this.queue = new ArrayBlockingQueue<>(capacity);
        if (enabled) for (int index = 0; index < workerCount; index++) {
            Thread worker = new Thread(this::dispatch, "seckill-eligibility-" + index);
            worker.setDaemon(true);
            workers.add(worker);
            worker.start();
        }
    }

    public SeckillAdmissionRepository.Eligibility inspect(Long user, Long voucher) {
        if (closed) throw new RateLimitExceededException(1000);
        if (!enabled) return jdbc.queryForObject("SELECT " + COLUMNS + "FROM (SELECT ? AS uid,? AS vid) r" + JOINS,
                (row, index) -> decode(row), user, voucher);
        Pending pending = new Pending(user, voucher);
        synchronized (lifecycle) {
            if (closed || !queue.offer(pending)) throw new RateLimitExceededException(1000);
        }
        try {
            return pending.result.get(waitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            queue.remove(pending);
            throw new RateLimitExceededException(1000);
        } catch (TimeoutException timeout) {
            queue.remove(pending);
            // This is before reservation/ID allocation: safe backpressure, not accepted work.
            throw new RateLimitExceededException(1000);
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof RuntimeException) throw (RuntimeException) failure.getCause();
            throw new IllegalStateException("Eligibility query failed", failure.getCause());
        }
    }

    private void dispatch() {
        while (!closed || !queue.isEmpty()) {
            List<Pending> batch = new ArrayList<>(size);
            try {
                Pending first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) continue;
                batch.add(first);
                long deadline = System.nanoTime() + flushNanos;
                while (batch.size() < size) {
                    Pending next = queue.poll();
                    if (next == null) {
                        long remaining = deadline - System.nanoTime();
                        if (remaining <= 0) break;
                        next = queue.poll(remaining, TimeUnit.NANOSECONDS);
                    }
                    if (next == null) break;
                    batch.add(next);
                }
                SeckillAdmissionRepository.Eligibility[] results = query(batch);
                for (int index = 0; index < batch.size(); index++) batch.get(index).result.complete(results[index]);
            } catch (InterruptedException interrupted) {
                for (Pending member : batch) member.result.completeExceptionally(new RateLimitExceededException(1000));
                if (closed) break;
            } catch (Exception error) {
                for (Pending member : batch) member.result.completeExceptionally(error);
            }
        }
    }

    private SeckillAdmissionRepository.Eligibility[] query(List<Pending> batch) {
        StringBuilder sql = new StringBuilder("SELECT r.slot," + COLUMNS + "FROM (");
        List<Object> arguments = new ArrayList<>(batch.size() * 3);
        for (int index = 0; index < batch.size(); index++) {
            if (index > 0) sql.append(" UNION ALL ");
            sql.append("SELECT ? AS slot,? AS uid,? AS vid");
            arguments.add(index);
            arguments.add(batch.get(index).user);
            arguments.add(batch.get(index).voucher);
        }
        sql.append(") r").append(JOINS);
        SeckillAdmissionRepository.Eligibility[] result = jdbc.query(sql.toString(), arguments.toArray(), rows -> {
            SeckillAdmissionRepository.Eligibility[] found = new SeckillAdmissionRepository.Eligibility[batch.size()];
            while (rows.next()) {
                int slot = rows.getInt("slot");
                if (slot < 0 || slot >= found.length || found[slot] != null)
                    throw new IllegalStateException("Ambiguous eligibility result");
                found[slot] = decode(rows);
            }
            for (SeckillAdmissionRepository.Eligibility item : found)
                if (item == null) throw new IllegalStateException("Missing eligibility result");
            return found;
        });
        return result;
    }

    private static SeckillAdmissionRepository.Eligibility decode(ResultSet row) throws SQLException {
        VoucherOrder order = null;
        if (row.getObject("order_id") != null) order = new VoucherOrder().setId(row.getLong("order_id"))
                .setUserId(row.getLong("user_id")).setVoucherId(row.getLong("voucher_id"))
                .setStatus(row.getInt("status")).setPayTime(row.getObject("pay_time", LocalDateTime.class))
                .setCreateTime(row.getObject("create_time", LocalDateTime.class));
        return new SeckillAdmissionRepository.Eligibility(order, row.getObject("begin_time", LocalDateTime.class),
                row.getObject("end_time", LocalDateTime.class));
    }

    @PreDestroy
    public void close() {
        synchronized (lifecycle) { closed = true; }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        for (Thread worker : workers) {
            try {
                long remaining = deadline - System.nanoTime();
                if (remaining > 0) worker.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
            } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }
        for (Thread worker : workers) if (worker.isAlive()) worker.interrupt();
        Pending pending;
        while ((pending = queue.poll()) != null) pending.result.completeExceptionally(new RateLimitExceededException(1000));
    }
}
