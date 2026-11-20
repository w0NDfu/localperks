package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.OrderTimeoutMessage;
import com.hmdp.dto.Result;
import com.hmdp.dto.SeckillOrderSubmitResult;
import com.hmdp.dto.VoucherOrderMessage;
import com.hmdp.dto.VoucherOrderStatusResult;
import com.hmdp.dto.VoucherWalletEntry;
import com.hmdp.entity.MqMessage;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.MqMessageMapper;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.mq.OrderTimeoutMessageService;
import com.hmdp.mq.SeckillOrderMessageService;
import com.hmdp.mq.SeckillAdmissionState;
import com.hmdp.ratelimit.RateLimitExceededException;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.hmdp.utils.MqMessageConstants.MQ_MESSAGE_STATUS_FAILED;
import static com.hmdp.utils.MqMessageConstants.MQ_MESSAGE_STATUS_SENT;
import static com.hmdp.utils.OrderConstants.ORDER_STATUS_CANCELLED;
import static com.hmdp.utils.OrderConstants.ORDER_STATUS_PAID;
import static com.hmdp.utils.OrderConstants.ORDER_STATUS_UNPAID;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_STATUS_CANCELLED;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_STATUS_FAILED;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_STATUS_PAID;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_STATUS_PENDING_PAY;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_STATUS_PROCESSING;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_STATUS_QUEUING;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_TEXT_CANCELLED;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_TEXT_FAILED;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_TEXT_PAID;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_TEXT_PENDING_PAY;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_TEXT_PROCESSING;
import static com.hmdp.utils.OrderStatusViewConstants.VIEW_TEXT_QUEUING;
import static com.hmdp.utils.RocketMQConstants.SECKILL_ORDER_TOPIC;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource private com.hmdp.ops.OpsJournal opsJournal;

    private static final String ORDER_LOCK_KEY_PREFIX = "lock:order:";

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final DefaultRedisScript<Long> SECKILL_ROLLBACK_SCRIPT;

    static {
        SECKILL_SCRIPT = com.hmdp.utils.RedisScripts.load("seckill.lua", Long.class);
        SECKILL_ROLLBACK_SCRIPT = com.hmdp.utils.RedisScripts.load("seckill_rollback.lua", Long.class);
    }

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private OrderTimeoutMessageService orderTimeoutMessageService;

    @Resource
    private SeckillOrderMessageService seckillOrderMessageService;

    @Resource
    private MqMessageMapper mqMessageMapper;

    @Resource
    private SeckillAdmissionState admissionState;

    @Value("${hmdp.seckill.max-pending:2000}")
    private int maxPending;

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                Long.toString(orderId),
                Long.toString(System.currentTimeMillis()),
                Integer.toString(maxPending)
        );
        int code = result == null ? 1 : result.intValue();
        if (code == 1) {
            return Result.fail("库存不足");
        }
        if (code == 2) {
            Long purchased = admissionState.purchased(voucherId, userId);
            if (purchased != null) {
                return Result.ok(new SeckillOrderSubmitResult(purchased, VIEW_STATUS_PROCESSING,
                        "已存在订单，请查看订单状态"));
            }
            Long prior = admissionState.previous(voucherId, userId);
            if (prior != null) {
                return submissionFromReceipt(prior, userId);
            }
            return Result.fail("抢券请求正在提交，请稍后查看我的券");
        }
        if (code == 3) {
            throw new RateLimitExceededException(1000);
        }
        if (code == 4) {
            return Result.fail("当前不在秒杀活动时段");
        }
        if (code == 5) {
            return Result.fail("秒杀活动暂未就绪，请稍后重试");
        }
        if (code != 0) {
            return Result.fail("抢券暂不可用，请稍后重试");
        }

        try {
            seckillOrderMessageService.createAndSend(new VoucherOrderMessage(orderId, userId, voucherId));
            return Result.ok(new SeckillOrderSubmitResult(orderId, VIEW_STATUS_QUEUING, VIEW_TEXT_QUEUING));
        } catch (Exception e) {
            // A send timeout may already have appended to the Broker. Retry only the original ID.
            log.warn("Broker确认结果待核对，保留原订单与预留，orderId={}", orderId, e);
            return submissionFromReceipt(orderId, userId);
        }

    }

    private Result submissionFromReceipt(Long orderId, Long userId) {
        Map<Object, Object> receipt = admissionState.receipt(orderId, userId);
        String state = String.valueOf(receipt.get("state"));
        if ("UNKNOWN".equals(state) || receipt.isEmpty()) {
            return Result.ok(new SeckillOrderSubmitResult(orderId, 19, "受理结果待核对"));
        }
        if ("FAILED".equals(state)) {
            return Result.ok(new SeckillOrderSubmitResult(orderId, VIEW_STATUS_FAILED, VIEW_TEXT_FAILED));
        }
        return Result.ok(new SeckillOrderSubmitResult(orderId, VIEW_STATUS_QUEUING, VIEW_TEXT_QUEUING));
    }

    @Override
    public Result queryOrderStatus(Long orderId) {
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = getById(orderId);
        if (order != null) {
            return userId.equals(order.getUserId()) ? Result.ok(orderStatusResult(order))
                    : Result.fail("订单请求不存在");
        }

        MqMessage mqMessage = mqMessageMapper.selectOne(new QueryWrapper<MqMessage>()
                .eq("order_id", orderId)
                .eq("user_id", userId)
                .eq("topic", SECKILL_ORDER_TOPIC)
                .last("limit 1"));
        if (mqMessage == null) {
            Map<Object, Object> receipt = admissionState.receipt(orderId, userId);
            if (!receipt.isEmpty()) {
                int status = "UNKNOWN".equals(receipt.get("state")) ? 19
                        : "FAILED".equals(receipt.get("state")) ? MQ_MESSAGE_STATUS_FAILED : MQ_MESSAGE_STATUS_SENT;
                return Result.ok(messageStatus(orderId, status, null));
            }
            return Result.fail("订单请求不存在");
        }
        if (mqMessage.getStatus() == 0 && mqMessage.getRetryCount() >= mqMessage.getMaxRetryCount()) {
            return Result.ok(new VoucherOrderStatusResult(orderId, 19, "投递结果待核对", null, mqMessage.getCreateTime()));
        }
        return Result.ok(messageStatus(orderId, mqMessage.getStatus(), mqMessage.getCreateTime()));
    }

    @Override
    public Result queryMine(int page, boolean ownedOnly) {
        Long userId = UserHolder.getUser().getId();
        Page<VoucherWalletEntry> request = new Page<>(Math.max(1, page), 12);
        request.setOptimizeCountSql(false);
        IPage<VoucherWalletEntry> history = getBaseMapper().queryWallet(request, userId, ownedOnly, SECKILL_ORDER_TOPIC,
                ownedOnly ? Collections.emptyList() : admissionState.recent(userId));
        for (VoucherWalletEntry entry : history.getRecords()) {
            VoucherOrderStatusResult state = entry.getOrderStatus() != null
                    ? orderStatusResult(entry.getOrderId(), entry.getOrderStatus(), entry.getPayTime(), entry.getCreateTime())
                    : messageStatus(entry.getOrderId(), entry.getMessageStatus(), entry.getCreateTime());
            entry.setStatusCode(state.getStatusCode());
            entry.setStatusText(state.getStatusText());
        }
        Map<String, Object> wallet = new LinkedHashMap<>();
        wallet.put("records", history.getRecords());
        wallet.put("total", history.getTotal());
        wallet.put("availableCount", ownedOnly ? history.getTotal() : query()
                .eq("user_id", userId).eq("status", ORDER_STATUS_PAID).count());
        return Result.ok(wallet);
    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrderMessage message) {
        Long userId = message.getUserId();
        Long voucherId = message.getVoucherId();
        RLock redisLock = redissonClient.getLock(ORDER_LOCK_KEY_PREFIX + userId);
        boolean locked = redisLock.tryLock();
        if (!locked) {
            throw new IllegalStateException("重复订单正在处理中，稍后重试");
        }

        try {
            MqMessage terminal = mqMessageMapper.selectOne(new QueryWrapper<MqMessage>()
                    .eq("order_id", message.getOrderId()).eq("topic", SECKILL_ORDER_TOPIC)
                    .eq("status", MQ_MESSAGE_STATUS_FAILED).last("limit 1"));
            if (terminal != null) {
                // A late duplicate must not recreate an order after terminal compensation.
                completeAdmissionAfterCommit(message, true);
                return;
            }
            VoucherOrder existing = query()
                    .eq("user_id", userId)
                    .eq("voucher_id", voucherId)
                    .one();
            if (existing != null) {
                if (opsJournal != null) opsJournal.record("order_idempotent", "orderId", message.getOrderId().toString(), "voucherId", voucherId.toString());
                if (Integer.valueOf(ORDER_STATUS_UNPAID).equals(existing.getStatus())) {
                    orderTimeoutMessageService.enqueue(existing.getId(), userId, voucherId);
                }
                completeAdmissionAfterCommit(message, false, existing.getId());
                return;
            }

            boolean stockDeducted = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId)
                    .gt("stock", 0)
                    .update();
            if (!stockDeducted) {
                markOrderMessageFailed(message, "MySQL 库存不足");
                completeAdmissionAfterCommit(message, true);
                log.error("MySQL 库存不足，提交后回补预留，voucherId={}, userId={}", voucherId, userId);
                return;
            }

            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setId(message.getOrderId());
            voucherOrder.setUserId(userId);
            voucherOrder.setVoucherId(voucherId);
            voucherOrder.setStatus(ORDER_STATUS_UNPAID);
            try {
                save(voucherOrder);
            } catch (DuplicateKeyException e) {
                seckillVoucherService.update()
                        .setSql("stock = stock + 1")
                        .eq("voucher_id", voucherId)
                        .update();
                log.warn("重复订单消息已被数据库唯一索引拦截，userId={}, voucherId={}", userId, voucherId);
                VoucherOrder actual = query().eq("user_id", userId).eq("voucher_id", voucherId).one();
                if (actual == null) throw e;
                completeAdmissionAfterCommit(message, false, actual.getId());
                return;
            }

            orderTimeoutMessageService.enqueue(message.getOrderId(), userId, voucherId);
            completeAdmissionAfterCommit(message, false);
        } finally {
            redisLock.unlock();
        }
    }

    private void completeAdmissionAfterCommit(VoucherOrderMessage message, boolean failed) {
        completeAdmissionAfterCommit(message, failed, message.getOrderId());
    }

    private void completeAdmissionAfterCommit(VoucherOrderMessage message, boolean failed, Long actualOrderId) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    if (failed) {
                        rollbackSeckillReservation(message.getVoucherId(), message.getUserId(), message.getOrderId());
                        if (opsJournal != null) opsJournal.record("reservation_compensated",
                                "orderId", message.getOrderId().toString(), "voucherId", message.getVoucherId().toString(),
                                "reason", "mysql_stock_unavailable");
                    }
                    admissionState.finish(message, failed ? "FAILED" : "SUCCESS", actualOrderId);
                } catch (Exception error) {
                    log.warn("Order committed; reservation receipt awaits reconciliation, orderId={}", message.getOrderId(), error);
                }
            }
        });
    }

    @Override
    @Transactional
    public void handleOrderTimeout(OrderTimeoutMessage message) {
        boolean cancelled = update()
                .set("status", ORDER_STATUS_CANCELLED)
                .eq("id", message.getOrderId())
                .eq("status", ORDER_STATUS_UNPAID)
                .update();
        if (!cancelled) {
            return;
        }

        seckillVoucherService.update()
                .setSql("stock = stock + 1")
                .eq("voucher_id", message.getVoucherId())
                .update();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    admissionState.releaseCancelled(message);
                } catch (Exception error) {
                    log.warn("Cancelled order awaits Redis release reconciliation, orderId={}", message.getOrderId(), error);
                }
            }
        });
        System.out.println("订单超时");
    }

    @Override
    @Transactional
    public Result cancelOrder(Long orderId) {
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = getById(orderId);
        if (order == null || !userId.equals(order.getUserId())) {
            return Result.fail("订单不存在");
        }
        if (Integer.valueOf(ORDER_STATUS_CANCELLED).equals(order.getStatus())) {
            return Result.ok(orderStatusResult(order));
        }
        if (!Integer.valueOf(ORDER_STATUS_UNPAID).equals(order.getStatus())) {
            return Result.fail("只有未支付订单可以取消");
        }
        // Reuse the existing CAS cancellation and stock restoration transaction.
        handleOrderTimeout(new OrderTimeoutMessage(orderId, userId, order.getVoucherId()));
        VoucherOrder current = getById(orderId);
        return Integer.valueOf(ORDER_STATUS_CANCELLED).equals(current.getStatus())
                ? Result.ok(orderStatusResult(current)) : Result.fail("订单状态已变化，取消未生效");
    }

    @Override
    @Transactional
    public Result mockPay(Long orderId, Boolean success) {
        Long userId = UserHolder.getUser().getId();
        VoucherOrder order = getById(orderId);
        if (order == null || !userId.equals(order.getUserId())) {
            return Result.fail("订单不存在");
        }
        if (!Boolean.TRUE.equals(success)) {
            System.out.println("支付失败");
            return Result.ok("支付失败");
        }

        boolean paid = update()
                .set("status", ORDER_STATUS_PAID)
                .set("pay_time", LocalDateTime.now())
                .eq("id", orderId)
                .eq("user_id", userId)
                .eq("status", ORDER_STATUS_UNPAID)
                .update();
        if (!paid) {
            return Result.fail("订单状态不允许支付");
        }

        System.out.println("支付成功");
        return Result.ok("支付成功");
    }

    private VoucherOrderStatusResult orderStatusResult(VoucherOrder order) {
        return orderStatusResult(order.getId(), order.getStatus(), order.getPayTime(), order.getCreateTime());
    }

    private VoucherOrderStatusResult orderStatusResult(Long id, Integer status,
                                                       LocalDateTime payTime, LocalDateTime createTime) {
        if (Integer.valueOf(ORDER_STATUS_PAID).equals(status)) {
            return new VoucherOrderStatusResult(id, VIEW_STATUS_PAID, VIEW_TEXT_PAID, payTime, createTime);
        }
        if (Integer.valueOf(ORDER_STATUS_UNPAID).equals(status)) {
            return new VoucherOrderStatusResult(id, VIEW_STATUS_PENDING_PAY, VIEW_TEXT_PENDING_PAY, payTime, createTime);
        }
        if (Integer.valueOf(3).equals(status)) {
            return new VoucherOrderStatusResult(id, 16, "已核销", payTime, createTime);
        }
        if (Integer.valueOf(5).equals(status)) {
            return new VoucherOrderStatusResult(id, 17, "退款中", payTime, createTime);
        }
        if (Integer.valueOf(6).equals(status)) {
            return new VoucherOrderStatusResult(id, 18, "已退款", payTime, createTime);
        }
        return new VoucherOrderStatusResult(id, VIEW_STATUS_CANCELLED, VIEW_TEXT_CANCELLED, payTime, createTime);
    }

    private VoucherOrderStatusResult messageStatus(Long id, Integer status, LocalDateTime createTime) {
        if (Integer.valueOf(19).equals(status)) {
            return new VoucherOrderStatusResult(id, 19, "受理结果待核对", null, createTime);
        }
        if (Integer.valueOf(MQ_MESSAGE_STATUS_FAILED).equals(status)) {
            return new VoucherOrderStatusResult(id, VIEW_STATUS_FAILED, VIEW_TEXT_FAILED, null, createTime);
        }
        if (Integer.valueOf(MQ_MESSAGE_STATUS_SENT).equals(status)) {
            return new VoucherOrderStatusResult(id, VIEW_STATUS_PROCESSING, VIEW_TEXT_PROCESSING, null, createTime);
        }
        return new VoucherOrderStatusResult(id, VIEW_STATUS_QUEUING, VIEW_TEXT_QUEUING, null, createTime);
    }

    private void markOrderMessageFailed(VoucherOrderMessage message, String reason) {
        // A terminal consumer receipt, not a pending admission Outbox. Commit before compensating Redis.
        mqMessageMapper.recordSeckillFailure(message.getOrderId(), message.getUserId(), message.getVoucherId(),
                SECKILL_ORDER_TOPIC, cn.hutool.json.JSONUtil.toJsonStr(message), reason);
    }

    private void rollbackSeckillReservation(Long voucherId, Long userId, Long orderId) {
        stringRedisTemplate.execute(
                SECKILL_ROLLBACK_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                orderId.toString()
        );
    }
}
