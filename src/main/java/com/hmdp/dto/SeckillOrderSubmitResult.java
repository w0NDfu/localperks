package com.hmdp.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SeckillOrderSubmitResult {

    @JsonSerialize(using = ToStringSerializer.class)
    private Long orderId;

    private Integer statusCode;

    private String statusText;

    public String getRequestState() {
        return requestState(statusCode);
    }

    public static String requestState(Integer code) {
        if (code == null || code == 19) {
            return "UNKNOWN";
        }
        if (code == 10 || code == 11) {
            return "ACCEPTED";
        }
        return code == 15 ? "FAILED" : "SUCCESS";
    }
}
