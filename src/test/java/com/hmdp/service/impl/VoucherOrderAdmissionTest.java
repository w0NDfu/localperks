package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderSubmitResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.dto.VoucherOrderStatusResult;
import com.hmdp.mapper.MqMessageMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.SeckillAdmissionRepository;
import com.hmdp.mq.SeckillAdmissionState;
import com.hmdp.mq.SeckillOrderMessageService;
import com.hmdp.ratelimit.RateLimitExceededException;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class VoucherOrderAdmissionTest {
    private final VoucherOrderServiceImpl service = new VoucherOrderServiceImpl();
    private final SeckillAdmissionRepository admission = mock(SeckillAdmissionRepository.class);
    private final SeckillOrderMessageService messages = mock(SeckillOrderMessageService.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final SeckillAdmissionState state = mock(SeckillAdmissionState.class);

    @BeforeEach
    void setup() {
        UserDTO user = new UserDTO();
        user.setId(1L);
        UserHolder.saveUser(user);
        ReflectionTestUtils.setField(service, "admissionState", state);
        ReflectionTestUtils.setField(service, "mqMessageMapper", mock(MqMessageMapper.class));
        ReflectionTestUtils.setField(service, "baseMapper", mock(VoucherOrderMapper.class));
        ReflectionTestUtils.setField(service, "seckillOrderMessageService", messages);
        ReflectionTestUtils.setField(service, "stringRedisTemplate", redis);
        ReflectionTestUtils.setField(service, "maxPending", 2000);
        RedisIdWorker ids = mock(RedisIdWorker.class);
        when(ids.nextId("order")).thenReturn(42L);
        ReflectionTestUtils.setField(service, "redisIdWorker", ids);
        when(state.purchased(anyLong(), anyLong())).thenReturn(null);
        when(state.previous(anyLong(), anyLong())).thenReturn(null);
    }

    @AfterEach
    void cleanup() {
        UserHolder.removeUser();
    }

    @Test
    void shouldKeepAnUnknownBrokerSendReservationAndReturnItsOriginalId() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(0L);
        doThrow(new IllegalStateException("Broker reply lost")).when(messages).createAndSend(any());
        Result result = service.seckillVoucher(7L);
        SeckillOrderSubmitResult receipt = (SeckillOrderSubmitResult) result.getData();
        assertTrue(result.getSuccess());
        assertEquals(42L, receipt.getOrderId());
        assertEquals("UNKNOWN", receipt.getRequestState());
        // Only the reserve Lua ran. An ambiguous send must never invoke rollback Lua.
        verify(redis, times(1)).execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString(), anyString());
        verify(redis, never()).execute(any(RedisScript.class), anyList(), anyString(), anyString());
    }

    @Test
    void shouldRejectBackpressureBeforePublishing() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(3L);
        assertThrows(RateLimitExceededException.class, () -> service.seckillVoucher(7L));
        verifyNoInteractions(messages);
    }

    @Test
    void shouldReturnAcceptedOnlyAfterConfirmedSendReturns() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(0L);
        Result result = service.seckillVoucher(7L);
        assertEquals("ACCEPTED", ((SeckillOrderSubmitResult) result.getData()).getRequestState());
        verify(messages).createAndSend(any());
    }

    @Test
    void shouldKeepOriginalIdOnDuplicateUnconfirmedRequest() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString(), anyString())).thenReturn(2L);
        when(state.previous(7L, 1L)).thenReturn(41L);
        when(state.receipt(41L, 1L)).thenReturn(Collections.singletonMap("state", "UNKNOWN"));
        SeckillOrderSubmitResult response = (SeckillOrderSubmitResult) service.seckillVoucher(7L).getData();
        assertEquals(41L, response.getOrderId());
        assertEquals("UNKNOWN", response.getRequestState());
        verifyNoInteractions(messages);
    }

    @Test
    void shouldReadConfirmedRequestBeforeOrderExistsWithoutSqlAdmissionRow() {
        when(state.receipt(42L, 1L)).thenReturn(Collections.singletonMap("state", "ACCEPTED"));
        Result result = service.queryOrderStatus(42L);
        assertTrue(result.getSuccess());
        assertEquals("ACCEPTED", ((VoucherOrderStatusResult) result.getData()).getRequestState());
    }

    @Test
    void shouldKeepUnknownStatusBeforeBrokerConfirmation() {
        when(state.receipt(42L, 1L)).thenReturn(Collections.singletonMap("state", "UNKNOWN"));
        assertEquals("UNKNOWN", ((VoucherOrderStatusResult) service.queryOrderStatus(42L).getData()).getRequestState());
    }

    @Test
    void shouldRejectStatusWithoutAnOwnedReceipt() {
        assertFalse(service.queryOrderStatus(42L).getSuccess());
    }

    @Test
    void shouldNotQuerySqlOnAnyAdmissionOutcome() {
        for (long code : new long[]{0, 1, 2, 4, 5}) {
            when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString(), anyString()))
                    .thenReturn(code);
            service.seckillVoucher(7L);
        }
        verifyNoInteractions(admission,
                ReflectionTestUtils.getField(service, "mqMessageMapper"),
                ReflectionTestUtils.getField(service, "baseMapper"));
    }

    @Test
    void shouldRejectMissingPreparedStateWithoutPublishingOrQueryingSql() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(5L);
        Result result = service.seckillVoucher(7L);
        assertFalse(result.getSuccess());
        assertTrue(result.getErrorMsg().contains("暂未就绪"));
        verifyNoInteractions(messages, admission);
    }

    @Test
    void shouldReturnPermanentPurchaseIdEvenAfterItsReceiptHasExpired() {
        when(redis.execute(any(RedisScript.class), anyList(), anyString(), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(2L);
        when(state.purchased(7L, 1L)).thenReturn(41L);
        SeckillOrderSubmitResult result = (SeckillOrderSubmitResult) service.seckillVoucher(7L).getData();
        assertEquals(41L, result.getOrderId());
        verifyNoInteractions(messages, admission);
        verify(state, never()).receipt(anyLong(), anyLong());
    }
}
