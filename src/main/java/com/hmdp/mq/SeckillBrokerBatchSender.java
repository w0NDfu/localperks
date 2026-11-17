package com.hmdp.mq;

import com.hmdp.dto.VoucherOrderMessage;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hmdp.utils.RocketMQConstants.SECKILL_ORDER_TOPIC;
import static com.hmdp.utils.RocketMQConstants.SEND_TIMEOUT_MILLIS;

/** Transient coalescing only: callers must receive the batch Broker ACK before acceptance. */
@Component
public class SeckillBrokerBatchSender {
    private final RocketMQTemplate rocket;
    private final boolean enabled;
    private final int batchSize;
    private final long flushNanos;
    private final long waitMillis;
    private final ArrayBlockingQueue<Pending> queue;
    private final List<Thread> workers = new ArrayList<>();
    private final Object lifecycle = new Object();
    private volatile boolean closed;

    private static class Pending {
        final VoucherOrderMessage message;
        final CompletableFuture<SendResult> result = new CompletableFuture<>();
        Pending(VoucherOrderMessage message) { this.message = message; }
    }

    public SeckillBrokerBatchSender(RocketMQTemplate rocket,
            @Value("${hmdp.seckill.broker-batch.enabled:true}") boolean enabled,
            @Value("${hmdp.seckill.broker-batch.workers:2}") int workerCount,
            @Value("${hmdp.seckill.broker-batch.size:32}") int batchSize,
            @Value("${hmdp.seckill.broker-batch.flush-millis:1}") long flushMillis,
            @Value("${hmdp.seckill.broker-batch.capacity:1024}") int capacity,
            @Value("${hmdp.seckill.broker-batch.wait-millis:3000}") long waitMillis) {
        // Payloads have exactly three Long fields; 32 members remain far below 1 MiB.
        if (workerCount < 1 || workerCount > 8 || batchSize < 1 || batchSize > 32
                || flushMillis < 0 || flushMillis > 10 || capacity < batchSize || capacity > 4096 || waitMillis < 1)
            throw new IllegalArgumentException("Invalid Broker batch budget");
        this.rocket = rocket;
        this.enabled = enabled;
        this.batchSize = batchSize;
        this.flushNanos = TimeUnit.MILLISECONDS.toNanos(flushMillis);
        this.waitMillis = waitMillis;
        this.queue = new ArrayBlockingQueue<>(capacity);
        if (enabled) {
            for (int index = 0; index < workerCount; index++) {
                Thread worker = new Thread(this::dispatch, "seckill-broker-batch-" + index);
                worker.setDaemon(true);
                workers.add(worker);
                worker.start();
            }
        }
    }

    public boolean isEnabled() { return enabled; }

    public SendResult send(VoucherOrderMessage message) {
        if (!enabled) {
            if (closed) throw new IllegalStateException("Broker sender closed");
            return rocket.syncSend(SECKILL_ORDER_TOPIC, MessageBuilder.withPayload(message).build(), SEND_TIMEOUT_MILLIS);
        }
        Pending pending = new Pending(message);
        synchronized (lifecycle) {
            if (closed || !queue.offer(pending))
                throw new IllegalStateException("Broker batch unavailable; request remains UNKNOWN");
        }
        try {
            return pending.result.get(waitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Broker confirmation interrupted; request remains UNKNOWN", interrupted);
        } catch (TimeoutException timeout) {
            // Do not cancel a possibly appended send. Redis recovery retains the original ID.
            throw new IllegalStateException("Broker confirmation timed out; request remains UNKNOWN", timeout);
        } catch (ExecutionException failure) {
            throw new IllegalStateException("Broker batch outcome uncertain", failure.getCause());
        }
    }

    private void dispatch() {
        while (!closed || !queue.isEmpty()) {
            List<Pending> batch = new ArrayList<>(batchSize);
            try {
                Pending first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first == null) continue;
                batch.add(first);
                long deadline = System.nanoTime() + flushNanos;
                while (batch.size() < batchSize) {
                    Pending next = queue.poll();
                    if (next == null) {
                        long remaining = deadline - System.nanoTime();
                        if (remaining <= 0) break;
                        next = queue.poll(remaining, TimeUnit.NANOSECONDS);
                    }
                    if (next == null) break;
                    batch.add(next);
                }
                List<Message<?>> messages = new ArrayList<>(batch.size());
                for (Pending member : batch) messages.add(MessageBuilder.withPayload(member.message).build());
                SendResult result = rocket.syncSend(SECKILL_ORDER_TOPIC, messages, SEND_TIMEOUT_MILLIS);
                for (Pending member : batch) member.result.complete(result);
            } catch (InterruptedException interrupted) {
                for (Pending member : batch) member.result.completeExceptionally(interrupted);
                if (closed) break;
            } catch (Exception error) {
                for (Pending member : batch) member.result.completeExceptionally(error);
            }
        }
    }

    @PreDestroy
    public void close() {
        synchronized (lifecycle) { closed = true; }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SEND_TIMEOUT_MILLIS + 200);
        for (Thread worker : workers) {
            try {
                long remaining = deadline - System.nanoTime();
                if (remaining > 0) worker.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
            } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }
        for (Thread worker : workers) if (worker.isAlive()) worker.interrupt();
        Pending pending;
        while ((pending = queue.poll()) != null)
            pending.result.completeExceptionally(new IllegalStateException("Sender stopped; request remains UNKNOWN"));
    }
}
