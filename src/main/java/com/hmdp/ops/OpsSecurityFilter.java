package com.hmdp.ops;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import javax.annotation.Resource;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class OpsSecurityFilter extends OncePerRequestFilter {
    public static final String ADMIN_VERIFIED = "localperks.ops.adminVerified";
    public static final String LIMIT_SCOPE = "localperks.ops.limitScope";
    @Value("${HMDP_OPS_ADMIN_TOKEN:}") private String adminToken;
    @Value("${hmdp.ops.enabled:false}") private boolean enabled;
    @Resource private OpsJournal journal;

    public static boolean authorized(String expected, String provided) {
        return expected != null && expected.length() >= 32 && provided != null && provided.length() <= 256 &&
                MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8), provided.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws ServletException, IOException {
        String path = request.getServletPath();
        if (path.isEmpty()) path = request.getRequestURI().substring(request.getContextPath().length());
        if (path.equals("/internal/ops") || path.startsWith("/internal/ops/")) {
            response.setHeader("Cache-Control", "no-store");
            if (!enabled) { response.setStatus(404); return; }
            if (!authorized(adminToken, request.getHeader("X-Ops-Token"))) { response.setStatus(403); return; }
            if (!"GET".equals(request.getMethod())) { response.setStatus(405); return; }
            request.setAttribute(ADMIN_VERIFIED, true);
        }
        long begin = System.nanoTime();
        try { chain.doFilter(request, response); }
        finally {
            if (path.matches("/voucher-order/seckill/[0-9]{1,19}")) {
                journal.record("http", "voucherId", path.substring(path.lastIndexOf('/') + 1),
                        "status", response.getStatus(), "limitScope", request.getAttribute(LIMIT_SCOPE),
                        "durationMs", (System.nanoTime() - begin) / 1000000);
            }
        }
    }
}
