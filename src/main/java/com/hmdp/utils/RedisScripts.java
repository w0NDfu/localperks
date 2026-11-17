package com.hmdp.utils;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.util.StreamUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/** Packaged Lua is immutable for this application version. Avoid file checks under getSha1's monitor. */
public final class RedisScripts {
    private RedisScripts() {
    }

    public static <T> DefaultRedisScript<T> load(String name, Class<T> resultType) {
        try (InputStream stream = new ClassPathResource(name).getInputStream()) {
            DefaultRedisScript<T> script = new DefaultRedisScript<>(
                    StreamUtils.copyToString(stream, StandardCharsets.UTF_8), resultType);
            script.getSha1();
            return script;
        } catch (IOException error) {
            throw new IllegalStateException("Packaged Redis script cannot be loaded: " + name, error);
        }
    }
}
