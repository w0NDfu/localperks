package com.hmdp.config;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;
import java.io.IOException;
import java.io.InputStream;

/** Load code before traffic so the first concurrent requests do not all take the NOSCRIPT fallback. */
@Component
public class SeckillScriptInitializer {
    private final StringRedisTemplate redis;

    public SeckillScriptInitializer(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initialize() throws IOException {
        for (String name : new String[]{"seckill.lua", "rate_limit_sliding_window.lua", "seckill_rollback.lua",
                "seckill_release_cancelled.lua", "seckill_preheat_claim.lua", "seckill_preheat_publish.lua"}) {
            byte[] script;
            try (InputStream stream = new ClassPathResource(name).getInputStream()) {
                script = StreamUtils.copyToByteArray(stream);
            }
            redis.execute((RedisCallback<String>) connection -> connection.scriptLoad(script));
        }
    }
}
