package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.hmdp.dto.OrderTimeoutMessage;
import com.hmdp.entity.MqMessage;
import com.hmdp.mapper.MqMessageMapper;
import com.hmdp.service.IVoucherOrderService;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hmdp.utils.MqMessageConstants.MQ_MESSAGE_STATUS_SENDING;
import static com.hmdp.utils.MqMessageConstants.MQ_MESSAGE_STATUS_SENT;
import static com.hmdp.utils.RocketMQConstants.ORDER_TIMEOUT_DELAY_LEVEL;
import static com.hmdp.utils.RocketMQConstants.ORDER_TIMEOUT_TOPIC;
import static com.hmdp.utils.RocketMQConstants.SEND_TIMEOUT_MILLIS;

@Slf4j
@Component
public class OrderTimeoutMessageService {
    private static final int BATCH_SIZE = 32;
    private static final int MAX_ATTEMPTS = 5;
    private static final int TIMEOUT_MINUTES = 5;

    private final MqMessageMapper messages;
    private final RocketMQTemplate rocket;
    private final ObjectProvider<IVoucherOrderService> orders;
    private final OrderTimeoutClaimRepository claims;
    private final Semaphore slots = new Semaphore(BATCH_SIZE);

    public OrderTimeoutMessageService(MqMessageMapper messages, RocketMQTemplate rocket,
                                      ObjectProvider<IVoucherOrderService> orders, OrderTimeoutClaimRepository claims) {
        this.messages = messages;
        this.rocket = rocket;
        this.orders = orders;
        this.claims = claims;
    }

    /** Persist in the order transaction. The dispatcher only sees committed rows. */
    public void enqueue(Long orderId, Long userId, Long voucherId) {
        OrderTimeoutMessage timeout = new OrderTimeoutMessage(orderId, userId, voucherId);
        messages.insertTimeoutIfAbsent(new MqMessage().setOrderId(orderId).setUserId(userId)
                .setVoucherId(voucherId).setTopic(ORDER_TIMEOUT_TOPIC).setMessageBody(JSONUtil.toJsonStr(timeout))
                .setStatus(MQ_MESSAGE_STATUS_SENDING).setRetryCount(0).setMaxRetryCount(MAX_ATTEMPTS)
                .setNextRetryTime(LocalDateTime.now()));
    }

    @Scheduled(fixedDelayString = "${hmdp.mq.timeout-dispatch-delay:100}")
    public void dispatch() {
        LocalDateTime now = LocalDateTime.now();
        int reserved = 0;
        while (reserved < BATCH_SIZE && slots.tryAcquire()) reserved++;
        if (reserved == 0) return;
        List<MqMessage> pending;
        try {
            pending = claims.claimDue(now, reserved);
        } catch (RuntimeException error) {
            slots.release(reserved);
            throw error;
        }
        slots.release(reserved - pending.size());
        int unprocessed = pending.size();
        try {
            for (MqMessage row : pending) {
                unprocessed--;
                boolean handedOff = false;
                try {
                    LocalDateTime deadline = row.getCreateTime().plusMinutes(TIMEOUT_MINUTES);
                    if (!now.isBefore(deadline)) {
                        finishLocally(row);
                    } else if (row.getRetryCount() >= MAX_ATTEMPTS) {
                        messages.update(null, pendingVersion(row.getId(), row.getRetryCount() + 1)
                                .set("next_retry_time", deadline));
                    } else {
                        handedOff = true;
                        publish(row, deadline);
                    }
                } finally {
                    if (!handedOff) slots.release();
                }
            }
        } finally {
            // Claimed but unpublished rows remain recoverable after their lease expires.
            slots.release(unprocessed);
        }
    }

    private void publish(MqMessage row, LocalDateTime deadline) {
        AtomicBoolean completed = new AtomicBoolean();
        SendCallback callback = new SendCallback() {
            @Override
            public void onSuccess(SendResult result) {
                if (!completed.compareAndSet(false, true)) return;
                try {
                    if (result.getSendStatus() == SendStatus.SEND_OK) {
                        complete(row, result.getMsgId());
                    } else {
                        retry(row, deadline, result.getSendStatus().name());
                    }
                } finally {
                    slots.release();
                }
            }

            @Override
            public void onException(Throwable error) {
                if (!completed.compareAndSet(false, true)) return;
                try {
                    retry(row, deadline, error.getClass().getSimpleName() + ": " + error.getMessage());
                } finally {
                    slots.release();
                }
            }
        };
        try {
            rocket.asyncSend(ORDER_TIMEOUT_TOPIC, MessageBuilder.withPayload(decode(row)).build(),
                    callback, SEND_TIMEOUT_MILLIS, ORDER_TIMEOUT_DELAY_LEVEL);
        } catch (RuntimeException error) {
            callback.onException(error);
        }
    }

    private void finishLocally(MqMessage row) {
        try {
            orders.getObject().handleOrderTimeout(decode(row));
            complete(row, "local-timeout");
            log.info("订单超时消息未确认，通过持久化任务核对订单状态，orderId={}", row.getOrderId());
        } catch (RuntimeException error) {
            retry(row, LocalDateTime.now().plusSeconds(5), error.getClass().getSimpleName());
        }
    }

    private OrderTimeoutMessage decode(MqMessage row) {
        return JSONUtil.toBean(row.getMessageBody(), OrderTimeoutMessage.class);
    }

    private void complete(MqMessage row, String messageId) {
        messages.update(null, pendingVersion(row.getId(), row.getRetryCount() + 1)
                .set("status", MQ_MESSAGE_STATUS_SENT).set("message_id", messageId).set("last_error", null));
    }

    private void retry(MqMessage row, LocalDateTime deadline, String reason) {
        LocalDateTime backoff = LocalDateTime.now().plusSeconds(5L << Math.min(row.getRetryCount(), 3));
        messages.update(null, pendingVersion(row.getId(), row.getRetryCount() + 1)
                .set("next_retry_time", backoff.isBefore(deadline) ? backoff : deadline)
                .set("last_error", reason.substring(0, Math.min(reason.length(), 1024))));
        log.warn("订单超时任务等待重试，orderId={}, attempt={}, reason={}", row.getOrderId(), row.getRetryCount() + 1, reason);
    }

    private UpdateWrapper<MqMessage> pendingVersion(Long id, int attempts) {
        return new UpdateWrapper<MqMessage>().eq("id", id)
                .eq("status", MQ_MESSAGE_STATUS_SENDING).eq("retry_count", attempts);
    }
}
