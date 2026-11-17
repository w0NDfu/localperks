package com.hmdp.perf;

import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.boot.web.embedded.tomcat.TomcatWebServer;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.apache.coyote.AbstractProtocol;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Queue timestamps wrap the existing executor. Its worker and queue budgets are preserved. */
@Configuration
public class TomcatQueueProbe implements WebMvcConfigurer {
    private ThreadPoolExecutor delegate;

    @EventListener
    public void initialized(WebServerInitializedEvent event) {
        if (!(event.getWebServer() instanceof TomcatWebServer)) return;
        org.apache.catalina.connector.Connector connector = ((TomcatWebServer) event.getWebServer()).getTomcat().getConnector();
        AbstractProtocol<?> protocol = (AbstractProtocol<?>) connector.getProtocolHandler();
        Executor original = protocol.getExecutor();
        if (!(original instanceof ThreadPoolExecutor)) return;
        delegate = (ThreadPoolExecutor) original;
        PerfProbe.executor = delegate;
        protocol.setExecutor(command -> {
            long submitted = System.nanoTime();
            original.execute(() -> {
                PerfProbe.queued(submitted, System.nanoTime());
                try {
                    command.run();
                } finally {
                    PerfProbe.clearQueue();
                }
            });
        });
    }

    @EventListener
    public void closing(ContextClosedEvent event) {
        if (delegate != null) delegate.shutdown();
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new HandlerInterceptor() {
            @Override
            public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception error) {
                PerfProbe.responseFinished();
            }
        }).order(Integer.MAX_VALUE);
    }
}
