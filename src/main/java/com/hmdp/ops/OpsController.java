package com.hmdp.ops;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import javax.annotation.Resource;
import java.util.*;

@RestController
@RequestMapping("/internal/ops")
public class OpsController {
    @Resource private OpsReadService service;

    @GetMapping("/{tool}")
    public Map<String, Object> read(@PathVariable String tool, @RequestParam long start,
            @RequestParam long end, @RequestParam Long voucherId,
            @RequestParam(defaultValue="127.0.0.1") String ip,
            @RequestAttribute(name=OpsSecurityFilter.ADMIN_VERIFIED, required=false) Boolean adminVerified) {
        if (!Boolean.TRUE.equals(adminVerified)) throw new ResponseStatusException(HttpStatus.FORBIDDEN);
        long now = System.currentTimeMillis();
        if (start < now - 86400000L || end > now + 5000 || end <= start || end - start > 900000 || voucherId <= 0 ||
                !ip.matches("[0-9a-fA-F:.]{3,45}")) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid bounded observation scope");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("schema", "ops.v1"); out.put("tool", tool); out.put("windowStart", start); out.put("windowEnd", end);
        out.put("observedAt", now); out.put("voucherId", voucherId.toString());
        out.put("data", service.read(tool, start, end, voucherId, ip));
        return out;
    }
}
