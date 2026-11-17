package com.hmdp.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hmdp.entity.MqMessage;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;

public interface MqMessageMapper extends BaseMapper<MqMessage> {
    @Insert("INSERT INTO tb_mq_message(order_id,user_id,voucher_id,topic,message_body,status,last_error) "
            + "VALUES(#{orderId},#{userId},#{voucherId},#{topic},#{body},2,#{reason}) "
            + "ON DUPLICATE KEY UPDATE status=2,last_error=VALUES(last_error)")
    int recordSeckillFailure(@Param("orderId") Long orderId, @Param("userId") Long userId,
                            @Param("voucherId") Long voucherId, @Param("topic") String topic,
                            @Param("body") String body, @Param("reason") String reason);

    @Insert("INSERT INTO tb_mq_message (order_id,user_id,voucher_id,topic,message_body,status,retry_count,max_retry_count,next_retry_time) "
            + "VALUES (#{orderId},#{userId},#{voucherId},#{topic},#{messageBody},#{status},#{retryCount},#{maxRetryCount},#{nextRetryTime}) "
            + "ON DUPLICATE KEY UPDATE id = id")
    int insertTimeoutIfAbsent(MqMessage message);
}
