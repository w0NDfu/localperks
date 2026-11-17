package com.hmdp.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class VoucherWalletEntry {
    @JsonSerialize(using = ToStringSerializer.class)
    private Long orderId;
    private Long voucherId;
    private Long shopId;
    private String shopName;
    private String title;
    private Long payValue;
    private Long actualValue;
    private LocalDateTime createTime;
    private LocalDateTime payTime;
    @JsonIgnore
    private Integer orderStatus;
    @JsonIgnore
    private Integer messageStatus;
    private Integer statusCode;
    private String statusText;
}
