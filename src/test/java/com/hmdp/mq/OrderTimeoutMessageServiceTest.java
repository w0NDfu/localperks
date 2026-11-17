package com.hmdp.mq;

import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.dto.OrderTimeoutMessage;
import com.hmdp.entity.MqMessage;
import com.hmdp.mapper.MqMessageMapper;
import com.hmdp.service.IVoucherOrderService;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.apache.rocketmq.client.producer.SendCallback;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.beans.factory.ObjectProvider;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Arrays;
import java.util.concurrent.Semaphore;

import static com.hmdp.utils.RocketMQConstants.ORDER_TIMEOUT_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;

class OrderTimeoutMessageServiceTest {
    private final MqMessageMapper messages = mock(MqMessageMapper.class);
    private final RocketMQTemplate rocket = mock(RocketMQTemplate.class);
    private final IVoucherOrderService orders = mock(IVoucherOrderService.class);
    private final OrderTimeoutClaimRepository claims = mock(OrderTimeoutClaimRepository.class);
    @SuppressWarnings("unchecked")
    private final ObjectProvider<IVoucherOrderService> provider = mock(ObjectProvider.class);
    private final OrderTimeoutMessageService service = new OrderTimeoutMessageService(messages, rocket, provider, claims);

    @BeforeEach
    void setup() {
        when(provider.getObject()).thenReturn(orders);
    }

    @Test
    void enqueueOnlyPersistsTheTimeoutIntent() {
        service.enqueue(12L, 34L, 56L);

        ArgumentCaptor<MqMessage> event = ArgumentCaptor.forClass(MqMessage.class);
        verify(messages).insertTimeoutIfAbsent(event.capture());
        assertEquals(12L, event.getValue().getOrderId());
        assertEquals(ORDER_TIMEOUT_TOPIC, event.getValue().getTopic());
        verifyNoInteractions(rocket);
    }

    @Test
    void expiredIntentRunsTheExistingIdempotentTimeoutHandler() {
        when(claims.claimDue(any(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(Collections.singletonList(pending()));
        when(messages.update(isNull(), any(UpdateWrapper.class))).thenReturn(1);

        service.dispatch();

        verify(orders).handleOrderTimeout(new OrderTimeoutMessage(12L, 34L, 56L));
        verifyNoInteractions(rocket);
    }

    @Test
    void losingTheLeaseSkipsSideEffects() {
        when(claims.claimDue(any(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(Collections.emptyList());

        service.dispatch();

        verifyNoInteractions(rocket, orders);
    }

    @Test
    void shouldReturnAllPermitsIfBatchClaimFails() {
        when(claims.claimDue(any(), org.mockito.ArgumentMatchers.anyInt()))
                .thenThrow(new IllegalStateException("SQL unavailable")).thenReturn(Collections.emptyList());
        assertThrows(IllegalStateException.class, service::dispatch);
        service.dispatch();
        verify(claims, times(2)).claimDue(any(), org.mockito.ArgumentMatchers.eq(32));
        assertEquals(32, ((Semaphore) ReflectionTestUtils.getField(service, "slots")).availablePermits());
    }

    @Test
    void shouldReleaseAPublicationSlotOnlyOnceForRepeatedCallbacks() {
        MqMessage row = pending().setCreateTime(LocalDateTime.now());
        when(claims.claimDue(any(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(Collections.singletonList(row));
        doAnswer(call -> {
            SendCallback callback = call.getArgument(2);
            callback.onException(new IllegalStateException("lost acknowledgement"));
            callback.onException(new IllegalStateException("duplicate callback"));
            return null;
        }).when(rocket).asyncSend(any(String.class), any(org.springframework.messaging.Message.class),
                any(SendCallback.class), org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyInt());
        service.dispatch();
        assertEquals(32, ((Semaphore) ReflectionTestUtils.getField(service, "slots")).availablePermits());
    }

    @Test
    void shouldReturnUnpublishedPermitsIfLocalFallbackCannotUpdateSql() {
        when(claims.claimDue(any(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(Arrays.asList(pending(), pending()));
        doThrow(new IllegalStateException("SQL unavailable")).when(orders).handleOrderTimeout(any());
        when(messages.update(isNull(), any(UpdateWrapper.class))).thenThrow(new IllegalStateException("SQL unavailable"));
        assertThrows(IllegalStateException.class, service::dispatch);
        assertEquals(32, ((Semaphore) ReflectionTestUtils.getField(service, "slots")).availablePermits());
    }

    private MqMessage pending() {
        return new MqMessage().setId(7L).setOrderId(12L).setRetryCount(0)
                .setCreateTime(LocalDateTime.now().minusMinutes(6))
                .setMessageBody("{\"orderId\":12,\"userId\":34,\"voucherId\":56}");
    }
}
