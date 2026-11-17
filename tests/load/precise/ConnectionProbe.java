package com.hmdp.perf;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.messaging.Message;
import com.hmdp.dto.VoucherOrderMessage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/** Test-only timing. No command arguments, SQL parameters or credentials are recorded. */
@Aspect
@Component
public class ConnectionProbe {
    private static final Set<String> COMMANDS = new HashSet<>(Arrays.asList(
            "hGetAll", "expire", "pExpire", "evalSha", "eval", "incr", "get", "set", "hGet", "hSet"));

    @Around("execution(* com.zaxxer.hikari.HikariDataSource.getConnection(..)) && target(source)")
    public Object database(ProceedingJoinPoint call, HikariDataSource source) throws Throwable {
        long started = System.nanoTime();
        try {
            return call.proceed();
        } finally {
            PerfProbe.duration("db_connection_acquire_" + ("admission".equals(source.getPoolName()) ? "admission" : "primary"), started);
        }
    }

    @Around("execution(* org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory.getConnection(..))")
    public Object redis(ProceedingJoinPoint call) throws Throwable {
        long started = System.nanoTime();
        RedisConnection connection;
        try {
            connection = (RedisConnection) call.proceed();
        } finally {
            PerfProbe.duration("redis_connection_acquire", started);
        }
        return Proxy.newProxyInstance(RedisConnection.class.getClassLoader(), new Class<?>[]{RedisConnection.class},
                (proxy, method, arguments) -> {
                    long before = System.nanoTime();
                    try {
                        return method.invoke(connection, arguments);
                    } catch (InvocationTargetException error) {
                        throw error.getCause();
                    } finally {
                        if (COMMANDS.contains(method.getName())) {
                            PerfProbe.duration("redis_command_" + method.getName(), before);
                        }
                    }
                });
    }

    @Around("execution(* org.apache.rocketmq.spring.core.RocketMQTemplate.asyncSend(..))")
    public Object publish(ProceedingJoinPoint call) throws Throwable {
        VoucherOrderMessage payload = null;
        for (Object argument : call.getArgs()) {
            if (argument instanceof Message && ((Message<?>) argument).getPayload() instanceof VoucherOrderMessage) {
                payload = (VoucherOrderMessage) ((Message<?>) argument).getPayload();
            }
        }
        long started = System.nanoTime();
        try {
            return call.proceed();
        } finally {
            if (payload != null) {
                PerfProbe.durationFor("mq_send_entry", started, payload.getUserId(), payload.getVoucherId(), payload.getOrderId());
            }
        }
    }
}
