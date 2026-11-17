package com.hmdp.mq;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.entity.MqMessage;
import com.hmdp.mapper.MqMessageMapper;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class OrderTimeoutClaimRepositoryTest {
    @Test
    void shouldCommitTheWholeLeaseBeforeReturningItToPublisher() {
        MqMessageMapper mapper = mock(MqMessageMapper.class);
        PlatformTransactionManager manager = mock(PlatformTransactionManager.class);
        SimpleTransactionStatus transaction = new SimpleTransactionStatus();
        when(manager.getTransaction(any())).thenAnswer(call -> {
            TransactionDefinition definition = call.getArgument(0);
            assertEquals(TransactionDefinition.ISOLATION_READ_COMMITTED, definition.getIsolationLevel());
            return transaction;
        });
        List<MqMessage> rows = Arrays.asList(new MqMessage().setId(1L).setRetryCount(0), new MqMessage().setId(2L).setRetryCount(3));
        when(mapper.selectList(any(QueryWrapper.class))).thenReturn(rows);
        when(mapper.update(isNull(), any(UpdateWrapper.class))).thenReturn(2);
        OrderTimeoutClaimRepository repository = new OrderTimeoutClaimRepository(mapper, manager);
        assertSame(rows, repository.claimDue(LocalDateTime.now(), 32));
        verify(manager).commit(transaction);
        verify(manager, never()).rollback(any());
        assertEquals(0, rows.get(0).getRetryCount());
        assertEquals(3, rows.get(1).getRetryCount());
    }

    @Test
    void shouldRollBackAndReturnNoPublishableBatchIfAnyLeaseWasNotClaimed() {
        MqMessageMapper mapper = mock(MqMessageMapper.class);
        PlatformTransactionManager manager = mock(PlatformTransactionManager.class);
        SimpleTransactionStatus transaction = new SimpleTransactionStatus();
        when(manager.getTransaction(any())).thenReturn(transaction);
        when(mapper.selectList(any(QueryWrapper.class))).thenReturn(Arrays.asList(new MqMessage().setId(1L), new MqMessage().setId(2L)));
        when(mapper.update(isNull(), any(UpdateWrapper.class))).thenReturn(1);
        OrderTimeoutClaimRepository repository = new OrderTimeoutClaimRepository(mapper, manager);
        assertThrows(IllegalStateException.class, () -> repository.claimDue(LocalDateTime.now(), 32));
        verify(manager).rollback(transaction);
        verify(manager, never()).commit(any());
    }
}
