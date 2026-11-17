package com.hmdp.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VoucherOrderJsonTest {

    @Test
    void shouldSerializeLargeOrderIdsAsExactStrings() {
        long orderId = 700_000_000_000_000_001L;
        ObjectMapper mapper = new ObjectMapper();

        JsonNode submission = mapper.valueToTree(new SeckillOrderSubmitResult(orderId, 10, "排队中"));
        JsonNode status = mapper.valueToTree(new VoucherOrderStatusResult(orderId, 12, "待支付", null, null));
        assertEquals("ACCEPTED", submission.get("requestState").asText());
        assertEquals("SUCCESS", status.get("requestState").asText());
        assertEquals("UNKNOWN", mapper.valueToTree(new SeckillOrderSubmitResult(orderId, 19, "待核对")).get("requestState").asText());

        assertTrue(submission.path("orderId").isTextual());
        assertTrue(status.path("orderId").isTextual());
        assertEquals(Long.toString(orderId), submission.path("orderId").asText());
        assertEquals(Long.toString(orderId), status.path("orderId").asText());
    }
}
