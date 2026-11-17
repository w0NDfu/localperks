package com.hmdp.perf;

import org.springframework.data.redis.core.script.DefaultRedisScript;

public class TimedRedisScript<T> extends DefaultRedisScript<T> {
    public TimedRedisScript() {
    }

    public TimedRedisScript(String source, Class<T> resultType) {
        super(source, resultType);
    }

    @Override
    public String getSha1() {
        long started = System.nanoTime();
        try {
            return super.getSha1();
        } finally {
            PerfProbe.duration("redis_sha_check", started);
        }
    }
}
