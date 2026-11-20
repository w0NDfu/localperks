package com.hmdp.mq;

import com.hmdp.entity.MqMessage;
import com.hmdp.entity.VoucherOrder;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.List;

import static com.hmdp.utils.RocketMQConstants.SECKILL_ORDER_TOPIC;

/** SQL access for background delivery recovery only; never called by normal admission. */
@Repository
public class SeckillAdmissionRepository {
    private final JdbcTemplate background;
    private static final BeanPropertyRowMapper<VoucherOrder> ORDER_MAPPER = new BeanPropertyRowMapper<>(VoucherOrder.class);
    private static final BeanPropertyRowMapper<MqMessage> MESSAGE_MAPPER = new BeanPropertyRowMapper<>(MqMessage.class);

    public SeckillAdmissionRepository(DataSource businessSource) {
        this.background = new JdbcTemplate(businessSource);
    }

    public VoucherOrder findOrder(Long userId, Long voucherId) {
        return first(background.query("SELECT * FROM tb_voucher_order WHERE user_id=? AND voucher_id=?",
                ORDER_MAPPER, userId, voucherId));
    }

    public MqMessage findMessage(Long orderId, Long userId) {
        return first(background.query("SELECT * FROM tb_mq_message WHERE order_id=? AND user_id=? AND topic=?",
                MESSAGE_MAPPER, orderId, userId, SECKILL_ORDER_TOPIC));
    }

    public List<MqMessage> due(LocalDateTime now) {
        return background.query("SELECT * FROM tb_mq_message WHERE topic=? AND status=0 AND next_retry_time<=? "
                        + "AND retry_count<max_retry_count ORDER BY next_retry_time,id LIMIT 64",
                MESSAGE_MAPPER, SECKILL_ORDER_TOPIC, now);
    }

    public boolean claim(MqMessage row) {
        return background.update("UPDATE tb_mq_message SET retry_count=retry_count+1,next_retry_time=? "
                        + "WHERE id=? AND status=0 AND retry_count=? AND next_retry_time<=?",
                LocalDateTime.now().plusSeconds(15), row.getId(), row.getRetryCount(), LocalDateTime.now()) == 1;
    }

    public void sent(Long id, String messageId) {
        background.update("UPDATE tb_mq_message SET status=1,message_id=?,last_error=NULL WHERE id=? AND status=0",
                messageId, id);
    }

    public void uncertain(MqMessage row, String error) {
        background.update("UPDATE tb_mq_message SET last_error=?,next_retry_time=? WHERE id=? AND status=0 AND retry_count=?",
                error.substring(0, Math.min(error.length(), 1024)),
                LocalDateTime.now().plusSeconds(Math.min(60, 1L << Math.min(row.getRetryCount(), 5))),
                row.getId(), row.getRetryCount());
    }

    private static <T> T first(List<T> rows) {
        return rows.isEmpty() ? null : rows.get(0);
    }
}
