package com.hmdp.service.impl;

import com.hmdp.cache.VoucherCacheInvalidationEvent;
import com.hmdp.cache.SeckillActivityCache;
import com.hmdp.entity.Voucher;
import com.hmdp.mapper.VoucherMapper;
import com.hmdp.service.ISeckillVoucherService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class VoucherServiceImplTest {

    @Mock
    private VoucherMapper voucherMapper;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    @Mock
    private ISeckillVoucherService seckillVoucherService;

    @Mock
    private SeckillActivityCache activityCache;

    private VoucherServiceImpl voucherService;

    @BeforeEach
    void setUp() {
        voucherService = new VoucherServiceImpl();
        ReflectionTestUtils.setField(voucherService, "baseMapper", voucherMapper);
        ReflectionTestUtils.setField(voucherService, "applicationEventPublisher", eventPublisher);
        ReflectionTestUtils.setField(voucherService, "seckillVoucherService", seckillVoucherService);
        ReflectionTestUtils.setField(voucherService, "seckillActivityCache", activityCache);
    }

    @Test
    void shouldPublishInvalidationEventAfterVoucherIsSaved() {
        Voucher voucher = new Voucher();
        voucher.setId(1L);
        voucher.setShopId(10L);
        when(voucherMapper.insert(voucher)).thenReturn(1);
        ArgumentCaptor<VoucherCacheInvalidationEvent> eventCaptor =
                ArgumentCaptor.forClass(VoucherCacheInvalidationEvent.class);

        assertTrue(voucherService.save(voucher));

        verify(eventPublisher).publishEvent(eventCaptor.capture());
        assertEquals(10L, eventCaptor.getValue().getShopId());
    }

    @Test
    void shouldPublishInvalidationEventAfterVoucherIsUpdated() {
        Voucher voucher = new Voucher();
        voucher.setId(2L);
        voucher.setShopId(20L);
        when(voucherMapper.updateById(voucher)).thenReturn(1);
        ArgumentCaptor<VoucherCacheInvalidationEvent> eventCaptor =
                ArgumentCaptor.forClass(VoucherCacheInvalidationEvent.class);

        assertTrue(voucherService.updateById(voucher));

        verify(eventPublisher).publishEvent(eventCaptor.capture());
        assertEquals(20L, eventCaptor.getValue().getShopId());
    }

    @Test
    void shouldForceSeckillTypeAndPreheatAfterCommit() {
        Voucher voucher = new Voucher();
        voucher.setId(3L);
        voucher.setShopId(30L);
        voucher.setType(0);
        voucher.setStock(10);
        voucher.setBeginTime(LocalDateTime.now());
        voucher.setEndTime(LocalDateTime.now().plusHours(1));
        when(voucherMapper.insert(voucher)).thenReturn(1);
        when(seckillVoucherService.save(any())).thenReturn(true);
        TransactionSynchronizationManager.initSynchronization();
        try {
            voucherService.addSeckillVoucher(voucher);
            assertEquals(1, voucher.getType());
            TransactionSynchronizationManager.getSynchronizations().get(0).afterCommit();
            verify(activityCache).warm(3L);
        } finally { TransactionSynchronizationManager.clearSynchronization(); }
    }
}
