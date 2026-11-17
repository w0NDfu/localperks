package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.dto.VoucherOrderStatusResult;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.utils.UserHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

class VoucherOrderOwnershipTest {
    private VoucherOrderServiceImpl service;

    @BeforeEach
    void setUp() {
        service = spy(new VoucherOrderServiceImpl());
        UserDTO user = new UserDTO();
        user.setId(1L);
        UserHolder.saveUser(user);
    }

    @AfterEach
    void tearDown() {
        UserHolder.removeUser();
    }

    @Test
    void shouldShowTheOwnersPendingOrder() {
        VoucherOrder order = new VoucherOrder().setId(42L).setUserId(1L).setStatus(1);
        doReturn(order).when(service).getById(42L);
        Result result = service.queryOrderStatus(42L);
        assertTrue(result.getSuccess());
        assertEquals(12, ((VoucherOrderStatusResult) result.getData()).getStatusCode());
    }

    @Test
    void shouldRejectReadingAnotherUsersOrder() {
        doReturn(new VoucherOrder().setId(42L).setUserId(2L).setStatus(1)).when(service).getById(42L);
        assertFalse(service.queryOrderStatus(42L).getSuccess());
    }

    @Test
    void shouldRejectPayingAnotherUsersOrder() {
        doReturn(new VoucherOrder().setId(42L).setUserId(2L).setStatus(1)).when(service).getById(42L);
        assertFalse(service.mockPay(42L, true).getSuccess());
    }

    @Test
    void shouldRejectCancellingAnotherUsersOrder() {
        doReturn(new VoucherOrder().setId(42L).setUserId(2L).setStatus(1)).when(service).getById(42L);
        assertFalse(service.cancelOrder(42L).getSuccess());
    }

    @Test
    void shouldRejectCancellingPaidOrders() {
        doReturn(new VoucherOrder().setId(42L).setUserId(1L).setStatus(2)).when(service).getById(42L);
        assertFalse(service.cancelOrder(42L).getSuccess());
    }

    @Test
    void shouldReturnCancelledStateForRepeatedCancellation() {
        doReturn(new VoucherOrder().setId(42L).setUserId(1L).setStatus(4)).when(service).getById(42L);
        assertTrue(service.cancelOrder(42L).getSuccess());
    }
}
