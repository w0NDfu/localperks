package com.hmdp.mq;

import cn.hutool.json.JSONUtil;
import com.hmdp.dto.VoucherOrderMessage;
import com.hmdp.entity.MqMessage;
import com.hmdp.entity.VoucherOrder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.MqMessageConstants.MQ_MESSAGE_STATUS_FAILED;
import static com.hmdp.utils.RocketMQConstants.SECKILL_ORDER_TOPIC;
import static com.hmdp.utils.RocketMQConstants.SEND_TIMEOUT_MILLIS;

@Slf4j
@Component
public class SeckillOrderMessageService {
    private static final DefaultRedisScript<Long> ROLLBACK =
            com.hmdp.utils.RedisScripts.load("seckill_rollback.lua", Long.class);
    private final SeckillAdmissionRepository admission;
    private final SeckillAdmissionState receipts;
    private final RocketMQTemplate rocketMQTemplate;
    private final StringRedisTemplate redis;
    private final Semaphore inFlight;
    private final SeckillBrokerBatchSender brokerSender;
    private final ThreadPoolExecutor recovery = new ThreadPoolExecutor(4, 4, 0, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(64), runnable -> {
                Thread thread = new Thread(runnable, "seckill-recovery");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());

    public SeckillOrderMessageService(SeckillAdmissionRepository admission, SeckillAdmissionState receipts,
            RocketMQTemplate rocketMQTemplate, StringRedisTemplate redis,
            @Value("${hmdp.seckill.max-inflight-sends:64}") int maxInFlight, SeckillBrokerBatchSender brokerSender) {
        if (maxInFlight < 1) throw new IllegalArgumentException("max-inflight-sends must be positive");
        this.admission = admission;
        this.receipts = receipts;
        this.rocketMQTemplate = rocketMQTemplate;
        this.redis = redis;
        this.inFlight = new Semaphore(maxInFlight);
        this.brokerSender = brokerSender;
    }

    /** New admissions never write a SQL Outbox. Success requires the Broker's SEND_OK. */
    public void createAndSend(VoucherOrderMessage message) {
        boolean permit = false;
        if (!brokerSender.isEnabled()) {
            try {
                permit = inFlight.tryAcquire(SEND_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                if (!permit) throw new IllegalStateException("Broker sender busy; request remains UNKNOWN");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Send interrupted; request remains UNKNOWN", interrupted);
            }
        }
        String token = UUID.randomUUID().toString();
        boolean claimed = false;
        try {
            claimed = receipts.claimSend(message.getOrderId(), token);
            if (!claimed) {
                Map<Object, Object> receipt = receipts.receipt(message.getOrderId(), message.getUserId());
                if ("ACCEPTED".equals(receipt.get("state")) || "SUCCESS".equals(receipt.get("state"))) return;
                throw new IllegalStateException("Request is unresolved or another sender owns its lease");
            }
            sendConfirmed(message);
            try {
                receipts.accepted(message.getOrderId());
            } catch (Exception error) {
                // The Broker has confirmed. A failed Redis acknowledgement cannot undo acceptance.
                log.warn("Broker confirmed; receipt awaits reconciliation, orderId={}", message.getOrderId(), error);
            }
        } finally {
            if (claimed) {
                try { receipts.releaseSend(message.getOrderId(), token); }
                catch (Exception error) { log.warn("Send lease will expire, orderId={}", message.getOrderId(), error); }
            }
            if (permit) inFlight.release();
        }
    }

    private SendResult sendConfirmed(VoucherOrderMessage message) {
        SendResult result = brokerSender.send(message);
        if (result == null || result.getSendStatus() != SendStatus.SEND_OK) {
            // Flush/replica timeouts may already have appended the message. Never compensate here.
            throw new IllegalStateException("Broker confirmation uncertain: " + (result == null ? "null" : result.getSendStatus()));
        }
        return result;
    }

    private void offer(Runnable work) {
        try {
            recovery.execute(() -> {
                try { work.run(); }
                catch (Exception error) { log.warn("Seckill request awaits recovery", error); }
            });
        } catch (RejectedExecutionException full) {
            // Redis receipts / pre-upgrade SQL rows remain available to the next scan.
            log.debug("Recovery busy; leave request for the next scan");
        }
    }

    /** Drain only pre-upgrade Outbox rows. New requests do not create these rows. */
    public void retryPendingMessages() {
        for (MqMessage row : admission.due(LocalDateTime.now())) offer(() -> publishLegacy(row));
    }

    private void publishLegacy(MqMessage row) {
        if (!inFlight.tryAcquire()) return;
        try {
            if (!admission.claim(row)) return;
            row.setRetryCount(row.getRetryCount() + 1);
            try {
                SendResult result = sendConfirmed(JSONUtil.toBean(row.getMessageBody(), VoucherOrderMessage.class));
                admission.sent(row.getId(), result.getMsgId());
            } catch (Exception error) {
                admission.uncertain(row, error.toString());
            }
        } finally { inFlight.release(); }
    }

    @Scheduled(fixedDelay = 1000)
    public void reconcileReceipts() {
        recover(receipts.due(true));
        recover(receipts.due(false));
    }

    private void recover(Set<String> ids) {
        if (ids != null) for (String id : ids) offer(() -> reconcile(id));
    }

    private void reconcile(String id) {
        VoucherOrderMessage message = receipts.message(id);
        VoucherOrder order = admission.findOrder(message.getUserId(), message.getVoucherId());
        MqMessage terminal = admission.findMessage(message.getOrderId(), message.getUserId());
        if (order != null) {
            receipts.finish(message, "SUCCESS", order.getId());
        } else if (terminal != null && Integer.valueOf(MQ_MESSAGE_STATUS_FAILED).equals(terminal.getStatus())) {
            redis.execute(ROLLBACK, Collections.emptyList(), message.getVoucherId().toString(),
                    message.getUserId().toString(), message.getOrderId().toString());
            receipts.finish(message, "FAILED");
        } else if ("UNKNOWN".equals(receipts.receipt(message.getOrderId(), message.getUserId()).get("state"))) {
            // Includes process exit between Redis reservation and send, and a lost Broker ACK.
            createAndSend(message);
        } else {
            // Confirmed messages are owned by RocketMQ; do not continuously republish its backlog.
            receipts.defer(message.getOrderId());
        }
    }

    @PreDestroy
    public void stop() { recovery.shutdown(); }
}
