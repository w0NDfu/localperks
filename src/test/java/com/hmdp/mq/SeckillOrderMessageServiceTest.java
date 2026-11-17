package com.hmdp.mq;

import com.hmdp.dto.VoucherOrderMessage;
import com.hmdp.entity.MqMessage;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.Message;
import java.util.Collections;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SeckillOrderMessageServiceTest {
    private final SeckillAdmissionRepository admission = mock(SeckillAdmissionRepository.class);
    private final SeckillAdmissionState state = mock(SeckillAdmissionState.class);
    private final RocketMQTemplate rocket = mock(RocketMQTemplate.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final SeckillBrokerBatchSender brokerSender = new SeckillBrokerBatchSender(rocket, false, 2, 32, 1, 1024, 3000);
    private final VoucherOrderMessage message = new VoucherOrderMessage(42L, 1L, 7L);

    private SendResult ack(SendStatus status) {
        SendResult result = new SendResult();
        result.setSendStatus(status);
        result.setMsgId("broker-id");
        return result;
    }

    @Test
    void shouldWaitForBrokerBeforeAcceptingWithoutAnySqlWrite() throws Exception {
        when(state.claimSend(eq(42L), anyString())).thenReturn(true);
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        when(rocket.syncSend(anyString(), any(Message.class), anyLong())).thenAnswer(call -> {
            entered.countDown();
            assertTrue(release.await(3, TimeUnit.SECONDS));
            return ack(SendStatus.SEND_OK);
        });
        SeckillOrderMessageService service = new SeckillOrderMessageService(admission, state, rocket, redis, 1, brokerSender);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            Future<?> request = worker.submit(() -> service.createAndSend(message));
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            assertFalse(request.isDone());
            verify(state, never()).accepted(anyLong());
            release.countDown();
            request.get(2, TimeUnit.SECONDS);
            verify(state).accepted(42L);
            verify(state).releaseSend(eq(42L), anyString());
            verifyNoInteractions(admission, redis);
        } finally { release.countDown(); worker.shutdownNow(); service.stop(); }
    }

    @Test
    void shouldKeepUnknownOnLostAckAndReleaseCapacityForRetry() {
        when(state.claimSend(eq(42L), anyString())).thenReturn(true);
        when(rocket.syncSend(anyString(), any(Message.class), anyLong()))
                .thenThrow(new IllegalStateException("Actual append may have succeeded"))
                .thenReturn(ack(SendStatus.SEND_OK));
        SeckillOrderMessageService service = new SeckillOrderMessageService(admission, state, rocket, redis, 1, brokerSender);
        try {
            assertThrows(IllegalStateException.class, () -> service.createAndSend(message));
            verify(state, never()).accepted(anyLong());
            service.createAndSend(message);
            verify(state).accepted(42L);
            verify(state, times(2)).releaseSend(eq(42L), anyString());
            verifyNoInteractions(admission, redis);
        } finally { service.stop(); }
    }

    @Test
    void shouldRejectFlushTimeoutWithoutConfirmingOrRollingBack() {
        when(state.claimSend(eq(42L), anyString())).thenReturn(true);
        when(rocket.syncSend(anyString(), any(Message.class), anyLong())).thenReturn(ack(SendStatus.FLUSH_DISK_TIMEOUT));
        SeckillOrderMessageService service = new SeckillOrderMessageService(admission, state, rocket, redis, 1, brokerSender);
        try {
            assertThrows(IllegalStateException.class, () -> service.createAndSend(message));
            verify(state, never()).accepted(anyLong());
            verifyNoInteractions(admission, redis);
        } finally { service.stop(); }
    }

    @Test
    void shouldKeepBrokerConfirmationWhenRedisUpdateFails() {
        when(state.claimSend(eq(42L), anyString())).thenReturn(true);
        when(rocket.syncSend(anyString(), any(Message.class), anyLong())).thenReturn(ack(SendStatus.SEND_OK));
        doThrow(new IllegalStateException("Redis unavailable")).when(state).accepted(42L);
        doThrow(new IllegalStateException("Redis unavailable")).when(state).releaseSend(eq(42L), anyString());
        SeckillOrderMessageService service = new SeckillOrderMessageService(admission, state, rocket, redis, 1, brokerSender);
        try { assertDoesNotThrow(() -> service.createAndSend(message)); }
        finally { service.stop(); }
    }

    @Test
    void shouldNotSendWhenAnotherInstanceOwnsUnknownRequest() {
        when(state.receipt(42L, 1L)).thenReturn(Collections.singletonMap("state", "UNKNOWN"));
        SeckillOrderMessageService service = new SeckillOrderMessageService(admission, state, rocket, redis, 1, brokerSender);
        try {
            assertThrows(IllegalStateException.class, () -> service.createAndSend(message));
            verifyNoInteractions(rocket, admission, redis);
            verify(state, never()).releaseSend(anyLong(), anyString());
        } finally { service.stop(); }
    }

    @Test
    void shouldReturnExistingConfirmationWithoutRepublishing() {
        when(state.receipt(42L, 1L)).thenReturn(Collections.singletonMap("state", "ACCEPTED"));
        SeckillOrderMessageService service = new SeckillOrderMessageService(admission, state, rocket, redis, 1, brokerSender);
        try {
            service.createAndSend(message);
            verifyNoInteractions(rocket, admission, redis);
        } finally { service.stop(); }
    }

    @Test
    void shouldRecoverReservationAfterExitBeforeSendUsingOriginalId() throws Exception {
        when(state.due(true)).thenReturn(Collections.singleton("42"));
        when(state.message("42")).thenReturn(message);
        when(state.receipt(42L, 1L)).thenReturn(Collections.singletonMap("state", "UNKNOWN"));
        when(state.claimSend(eq(42L), anyString())).thenReturn(true);
        when(rocket.syncSend(anyString(), any(Message.class), anyLong())).thenReturn(ack(SendStatus.SEND_OK));
        SeckillOrderMessageService service = new SeckillOrderMessageService(admission, state, rocket, redis, 1, brokerSender);
        try {
            service.reconcileReceipts();
            verify(state, timeout(2000)).accepted(42L);
            verify(rocket).syncSend(eq("seckill-order-topic"), argThat((Message<?> sent) ->
                    ((VoucherOrderMessage) sent.getPayload()).getOrderId().equals(42L)), eq(3000L));
        } finally { service.stop(); }
    }

    @Test
    void shouldRepairTerminalCompensationWithoutResending() {
        when(state.due(false)).thenReturn(Collections.singleton("42"));
        when(state.message("42")).thenReturn(message);
        when(admission.findMessage(42L, 1L)).thenReturn(new MqMessage().setStatus(2));
        SeckillOrderMessageService service = new SeckillOrderMessageService(admission, state, rocket, redis, 1, brokerSender);
        try {
            service.reconcileReceipts();
            verify(state, timeout(2000)).finish(message, "FAILED");
            verify(redis).execute(any(org.springframework.data.redis.core.script.RedisScript.class),
                    anyList(), eq("7"), eq("1"), eq("42"));
            verifyNoInteractions(rocket);
        } finally { service.stop(); }
    }

    @Test
    void shouldUseBoundedBatchQueueWithoutWaitingForLegacySingleSendPermits() throws Exception {
        SeckillBrokerBatchSender batches = mock(SeckillBrokerBatchSender.class);
        when(batches.isEnabled()).thenReturn(true);
        when(state.claimSend(anyLong(), anyString())).thenReturn(true);
        CountDownLatch bothEntered = new CountDownLatch(2), release = new CountDownLatch(1);
        when(batches.send(any())).thenAnswer(call -> {
            bothEntered.countDown();
            assertTrue(release.await(2, TimeUnit.SECONDS));
            return ack(SendStatus.SEND_OK);
        });
        SeckillOrderMessageService service = new SeckillOrderMessageService(admission, state, rocket, redis, 1, batches);
        ExecutorService callers = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = callers.submit(() -> service.createAndSend(message));
            Future<?> second = callers.submit(() -> service.createAndSend(new VoucherOrderMessage(43L, 2L, 7L)));
            assertTrue(bothEntered.await(1, TimeUnit.SECONDS));
            verify(state, never()).accepted(anyLong());
            release.countDown();
            first.get(1, TimeUnit.SECONDS);
            second.get(1, TimeUnit.SECONDS);
            verify(state).accepted(42L);
            verify(state).accepted(43L);
            verifyNoInteractions(rocket, admission, redis);
        } finally { release.countDown(); callers.shutdownNow(); service.stop(); }
    }
}

