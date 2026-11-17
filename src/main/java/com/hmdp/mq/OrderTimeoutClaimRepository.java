package com.hmdp.mq;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.entity.MqMessage;
import com.hmdp.mapper.MqMessageMapper;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.hmdp.utils.MqMessageConstants.MQ_MESSAGE_STATUS_SENDING;
import static com.hmdp.utils.RocketMQConstants.ORDER_TIMEOUT_TOPIC;

/** Lease a small batch atomically; release every SQL lock before network publication. MySQL 8+. */
@Repository
public class OrderTimeoutClaimRepository {
    private final MqMessageMapper messages;
    private final TransactionTemplate transaction;

    public OrderTimeoutClaimRepository(MqMessageMapper messages, PlatformTransactionManager manager) {
        this.messages = messages;
        transaction = new TransactionTemplate(manager);
        // Queue scans must not take next-key gap locks that conflict with concurrent enqueues.
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transaction.setTimeout(5);
    }

    public List<MqMessage> claimDue(LocalDateTime now, int capacity) {
        if (capacity < 1 || capacity > 32) throw new IllegalArgumentException("Invalid timeout lease budget");
        return transaction.execute(status -> {
            List<MqMessage> rows = messages.selectList(new QueryWrapper<MqMessage>()
                    .eq("topic", ORDER_TIMEOUT_TOPIC).eq("status", MQ_MESSAGE_STATUS_SENDING)
                    .le("next_retry_time", now).orderByAsc("next_retry_time", "id")
                    .last("LIMIT " + capacity + " FOR UPDATE SKIP LOCKED"));
            if (rows.isEmpty()) return rows;
            List<Long> ids = new ArrayList<>(rows.size());
            for (MqMessage row : rows) ids.add(row.getId());
            int changed = messages.update(null, new UpdateWrapper<MqMessage>().in("id", ids)
                    .eq("topic", ORDER_TIMEOUT_TOPIC).eq("status", MQ_MESSAGE_STATUS_SENDING)
                    .setSql("retry_count = retry_count + 1").set("next_retry_time", now.plusSeconds(12)));
            if (changed != rows.size()) throw new IllegalStateException("Timeout batch lease did not cover all locked rows");
            // Keep the pre-claim version in returned rows; callbacks match retry_count + 1.
            return rows;
        });
    }
}
