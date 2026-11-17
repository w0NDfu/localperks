package com.hmdp.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VoucherOrderStatusResult {

    @JsonSerialize(using = ToStringSerializer.class)
    private Long orderId;

    private Integer statusCode;

    private String statusText;

    private LocalDateTime payTime;

    private LocalDateTime createTime;

    public String getRequestState() {
        return SeckillOrderSubmitResult.requestState(statusCode);
    }
}
