package com.hmdp.mq;

import com.hmdp.dto.VoucherOrderMessage;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SeckillBrokerBatchSenderTest {
    private final RocketMQTemplate rocket = mock(RocketMQTemplate.class);
    private final SendResult confirmed = new SendResult();

    private VoucherOrderMessage message(long id) { return new VoucherOrderMessage(id, id, 7L); }

    @Test
    void shouldNotCompleteAnyCallerBeforeBatchBrokerAcknowledgement() throws Exception {
        confirmed.setSendStatus(SendStatus.SEND_OK);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        when(rocket.syncSend(anyString(), anyCollection(), anyLong())).thenAnswer(call -> {
            entered.countDown(); assertTrue(release.await(2, TimeUnit.SECONDS)); return confirmed;
        });
        SeckillBrokerBatchSender sender = new SeckillBrokerBatchSender(rocket, true, 1, 32, 1, 64, 3000);
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            Future<SendResult> result = caller.submit(() -> sender.send(message(1)));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            assertFalse(result.isDone());
            release.countDown();
            assertSame(confirmed, result.get(1, TimeUnit.SECONDS));
        } finally { release.countDown(); caller.shutdownNow(); sender.close(); }
    }

    @Test
    void shouldCoalesceWithoutLosingOrDuplicatingMembers() throws Exception {
        List<Long> ids = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger calls = new AtomicInteger();
        when(rocket.syncSend(anyString(), anyCollection(), anyLong())).thenAnswer(call -> {
            Collection<Message<?>> messages = call.getArgument(1);
            assertTrue(messages.size() <= 32);
            for (Message<?> member : messages) ids.add(((VoucherOrderMessage) member.getPayload()).getOrderId());
            calls.incrementAndGet(); return confirmed;
        });
        SeckillBrokerBatchSender sender = new SeckillBrokerBatchSender(rocket, true, 1, 32, 10, 128, 3000);
        ExecutorService callers = Executors.newFixedThreadPool(32);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (long id = 1; id <= 64; id++) {
                final long orderId = id;
                results.add(callers.submit(() -> { start.await(); return sender.send(message(orderId)); }));
            }
            start.countDown();
            for (Future<?> result : results) result.get(2, TimeUnit.SECONDS);
            assertEquals(64, ids.size());
            assertEquals(64, new HashSet<>(ids).size());
            assertTrue(calls.get() < 64);
        } finally { start.countDown(); callers.shutdownNow(); sender.close(); }
    }

    @Test
    void shouldFailTheAffectedBatchAndContinueWithLaterRequests() {
        when(rocket.syncSend(anyString(), anyCollection(), anyLong()))
                .thenThrow(new IllegalStateException("ACK lost")).thenReturn(confirmed);
        SeckillBrokerBatchSender sender = new SeckillBrokerBatchSender(rocket, true, 1, 1, 0, 8, 3000);
        try {
            assertThrows(IllegalStateException.class, () -> sender.send(message(1)));
            assertSame(confirmed, sender.send(message(2)));
        } finally { sender.close(); }
    }

    @Test
    void shouldKeepTimedOutWorkAndBoundTheRemainingQueue() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1), drained = new CountDownLatch(2);
        when(rocket.syncSend(anyString(), anyCollection(), anyLong())).thenAnswer(call -> {
            entered.countDown(); release.await(2, TimeUnit.SECONDS); drained.countDown(); return confirmed;
        });
        SeckillBrokerBatchSender sender = new SeckillBrokerBatchSender(rocket, true, 1, 1, 0, 1, 40);
        try {
            assertThrows(IllegalStateException.class, () -> sender.send(message(1)));
            assertTrue(entered.await(1, TimeUnit.SECONDS));
            assertThrows(IllegalStateException.class, () -> sender.send(message(2)));
            IllegalStateException full = assertThrows(IllegalStateException.class, () -> sender.send(message(3)));
            assertTrue(full.getMessage().contains("unavailable"));
            release.countDown();
            assertTrue(drained.await(1, TimeUnit.SECONDS));
            verify(rocket, times(2)).syncSend(anyString(), anyCollection(), anyLong());
        } finally { release.countDown(); sender.close(); }
    }

    @Test
    void shouldRejectAfterShutdown() {
        SeckillBrokerBatchSender sender = new SeckillBrokerBatchSender(rocket, true, 1, 1, 0, 1, 3000);
        sender.close();
        assertThrows(IllegalStateException.class, () -> sender.send(message(1)));
        verifyNoInteractions(rocket);
    }
}
