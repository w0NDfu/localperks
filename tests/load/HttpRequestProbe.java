package com.hmdp.perf;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/** Test-only filter. Transport scheduling before this filter is outside its timer. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class HttpRequestProbe extends OncePerRequestFilter {
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        long started = System.nanoTime();
        long entered = System.currentTimeMillis();
        try {
            chain.doFilter(request, response);
        } finally {
            if (request.getRequestURI().startsWith("/voucher-order/seckill/")) {
                PerfProbe.duration("http_filter_service", started);
                String token = request.getHeader("authorization");
                if (token != null && token.matches("[a-zA-Z0-9-]+-(?:load-)?user-[0-9]+")) {
                    String suffix = token.substring(token.lastIndexOf('-') + 1);
                    String uid = token.contains("-load-user-") ? suffix : Integer.toString(10000 + Integer.parseInt(suffix));
                    PerfProbe.http(entered, uid,
                            (System.nanoTime() - started) / 1000);
                }
            }
        }
    }
}
