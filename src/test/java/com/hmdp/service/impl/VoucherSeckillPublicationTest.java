package com.hmdp.service.impl;

import com.hmdp.cache.SeckillActivityCache;
import com.hmdp.entity.Voucher;
import com.hmdp.service.ISeckillVoucherService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class VoucherSeckillPublicationTest {
    private final VoucherServiceImpl service = spy(new VoucherServiceImpl());
    private final SeckillActivityCache cache = mock(SeckillActivityCache.class);
    private final ISeckillVoucherService seckill = mock(ISeckillVoucherService.class);
    private Voucher voucher;

    @BeforeEach
    void setup() {
        TransactionSynchronizationManager.initSynchronization();
        ReflectionTestUtils.setField(service, "seckillActivityCache", cache);
        ReflectionTestUtils.setField(service, "seckillVoucherService", seckill);
        voucher = new Voucher().setId(7L).setStock(10).setBeginTime(LocalDateTime.now())
                .setEndTime(LocalDateTime.now().plusHours(1));
        doReturn(true).when(service).save(voucher);
    }

    @AfterEach
    void cleanup() { TransactionSynchronizationManager.clearSynchronization(); }

    @Test
    void shouldPublishOnlyAfterTheDatabaseTransactionCommits() {
        service.addSeckillVoucher(voucher);
        verifyNoInteractions(cache);
        assertEquals(1, TransactionSynchronizationManager.getSynchronizations().size());
        TransactionSynchronizationManager.getSynchronizations().get(0).afterCommit();
        verify(cache).warm(7L);
    }

    @Test
    void shouldNotPublishRolledBackActivity() {
        service.addSeckillVoucher(voucher);
        TransactionSynchronizationManager.getSynchronizations().get(0)
                .afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK);
        verifyNoInteractions(cache);
    }

    @Test
    void shouldLeaveCommittedActivityForBackgroundRetryWhenRedisIsUnavailable() {
        service.addSeckillVoucher(voucher);
        doThrow(new IllegalStateException("Redis unavailable")).when(cache).warm(7L);
        assertDoesNotThrow(() -> TransactionSynchronizationManager.getSynchronizations().get(0).afterCommit());
        verify(cache).warm(7L);
    }

    @Test
    void shouldRejectInvalidActivityBeforeSavingOrWarmingIt() {
        voucher.setEndTime(voucher.getBeginTime());
        assertThrows(IllegalArgumentException.class, () -> service.addSeckillVoucher(voucher));
        verify(service, never()).save(voucher);
        verifyNoInteractions(seckill, cache);
    }
}
