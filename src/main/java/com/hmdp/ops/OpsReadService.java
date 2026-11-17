package com.hmdp.ops;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import javax.annotation.Resource;
import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.sql.Timestamp;
import java.util.*;
import static com.hmdp.utils.RocketMQConstants.*;

/** Fixed SELECT/GET/ZCOUNT/SCARD and broker query operations; no arbitrary commands. */
@Service
public class OpsReadService {
    @Resource private StringRedisTemplate redis;
    @Resource private DataSource dataSource;
    @Resource private RocketMQTemplate rocket;
    @Resource private OpsJournal journal;

    public Map<String, Object> read(String tool, long start, long end, Long voucher, String ip) {
        Map<String, Object> out = new LinkedHashMap<>();
        if ("events".equals(tool) || "traffic".equals(tool) || "delivery".equals(tool)) {
            out.putAll(journal.window(start, end, voucher));
            if ("traffic".equals(tool)) {
                out.put("ipWindowCount", redis.opsForZSet().count("rate:sliding:{voucher-seckill}:ip:" + ip, end - 60000, end));
                out.put("globalWindowCount", redis.opsForZSet().count("rate:sliding:{voucher-seckill}:global:all", end - 60000, end));
                out.put("ipLimit", 30); out.put("globalLimit", 300); out.put("windowMillis", 60000);
            }
            if ("delivery".equals(tool)) {
                out.put("orders", database().queryForList("SELECT CAST(id AS CHAR) AS orderId,status FROM tb_voucher_order WHERE voucher_id=? ORDER BY id LIMIT 50", voucher));
            }
        } else if ("spring".equals(tool)) {
            out.put("uptimeMs", ManagementFactory.getRuntimeMXBean().getUptime());
            out.put("heapUsedBytes", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
            out.put("threads", ManagementFactory.getThreadMXBean().getThreadCount());
            out.put("journal", journal.window(start, end, voucher));
        } else if ("inventory".equals(tool)) {
            out.put("mysql", database().queryForList("SELECT stock FROM tb_seckill_voucher WHERE voucher_id=?", voucher));
            out.put("redisStock", redis.opsForValue().get("seckill:stock:" + voucher));
            out.put("redisQualified", redis.opsForSet().size("seckill:order:" + voucher));
            out.put("orders", database().queryForList("SELECT COUNT(*) AS total,COALESCE(SUM(status=4),0) AS cancelled FROM tb_voucher_order WHERE voucher_id=?", voucher));
            Long reserved = redis.opsForZSet().size("seckill:admission:voucher:" + voucher);
            out.put("pending", Collections.singletonList(Collections.singletonMap("total", reserved == null ? 0L : reserved)));
            out.put("legacyPending", database().queryForList("SELECT COUNT(*) AS total FROM tb_mq_message m LEFT JOIN tb_voucher_order o ON o.id=m.order_id WHERE m.voucher_id=? AND m.topic=? AND m.status<>2 AND o.id IS NULL", voucher, SECKILL_ORDER_TOPIC));
            out.put("semantics", "Current stock snapshots; pending reservations and compensation must be checked before calling a mismatch drift");
        } else if ("compensation".equals(tool)) {
            out.put("outbox", database().queryForList("SELECT status,COUNT(*) AS total,MAX(retry_count) AS maxRetries FROM tb_mq_message WHERE voucher_id=? AND topic=? AND create_time BETWEEN ? AND ? GROUP BY status", voucher, SECKILL_ORDER_TOPIC, new Timestamp(start), new Timestamp(end)));
            out.put("journal", journal.window(start, end, voucher));
        } else if ("mq".equals(tool)) {
            try {
                MQClientInstance factory = rocket.getProducer().getDefaultMQProducerImpl().getmQClientFactory();
                Set<String> brokers = new HashSet<>();
                rocket.getProducer().fetchPublishMessageQueues(SECKILL_ORDER_TOPIC).forEach(q -> brokers.add(q.getBrokerName()));
                MQClientAPIImpl api = factory.getMQClientAPIImpl();
                long lag = 0, brokerOffset = 0, consumerOffset = 0;
                boolean enabled = true;
                int queues = 0;
                for (String broker : brokers) {
                    String address = factory.findBrokerAddressInPublish(broker);
                    if (address == null) throw new IllegalStateException("Broker route missing");
                    ConsumeStats stats = api.getConsumeStats(address, SECKILL_ORDER_CONSUMER_GROUP, SECKILL_ORDER_TOPIC, 1500);
                    for (org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper offset : stats.getOffsetTable().values()) {
                        brokerOffset += offset.getBrokerOffset(); consumerOffset += offset.getConsumerOffset();
                        lag += Math.max(0, offset.getBrokerOffset() - offset.getConsumerOffset()); queues++;
                    }
                    SubscriptionGroupConfig cfg = api.getSubscriptionGroupConfig(address, SECKILL_ORDER_CONSUMER_GROUP, 1500);
                    enabled &= cfg != null && cfg.isConsumeEnable();
                }
                out.put("topic", SECKILL_ORDER_TOPIC); out.put("group", SECKILL_ORDER_CONSUMER_GROUP);
                out.put("brokerOffset", brokerOffset); out.put("consumerOffset", consumerOffset); out.put("lag", lag);
                out.put("consumeEnabled", enabled); out.put("queues", queues); out.put("brokers", brokers.size());
                if (queues == 0) throw new IllegalStateException("No queue offsets");
            } catch (Exception error) { throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Broker query unavailable; cannot infer zero lag"); }
        } else throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown read-only tool");
        return out;
    }

    private JdbcTemplate database() {
        JdbcTemplate template = new JdbcTemplate(dataSource);
        template.setQueryTimeout(3); template.setMaxRows(50);
        return template;
    }
}
