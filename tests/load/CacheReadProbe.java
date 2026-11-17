package com.hmdp.perf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import java.io.BufferedWriter;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/** Read-only counters copied only into the isolated benchmark JAR. */
@Component
public class CacheReadProbe {
    public static final LongAdder requests = new LongAdder();
    public static final LongAdder l1Hits = new LongAdder();
    public static final LongAdder l2Lookups = new LongAdder();
    public static final LongAdder databaseFallbacks = new LongAdder();
    public static final LongAdder bloomBypass = new LongAdder();
    public static final LongAdder bloomErrors = new LongAdder();

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        Thread sampler = new Thread(() -> {
            ObjectMapper json = new ObjectMapper();
            try (BufferedWriter output = Files.newBufferedWriter(Paths.get(System.getenv("SHOPLINK_CACHE_METRICS")), StandardCharsets.UTF_8)) {
                while (!Thread.currentThread().isInterrupted()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("at", System.currentTimeMillis());
                    row.put("requests", requests.sum()); row.put("l1Hits", l1Hits.sum());
                    row.put("l2Lookups", l2Lookups.sum()); row.put("databaseFallbacks", databaseFallbacks.sum());
                    row.put("bloomBypass", bloomBypass.sum()); row.put("bloomErrors", bloomErrors.sum());
                    row.put("heapUsed", ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
                    row.put("threads", ManagementFactory.getThreadMXBean().getThreadCount());
                    row.put("processCpuNanos", ((com.sun.management.OperatingSystemMXBean)
                            ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime());
                    output.write(json.writeValueAsString(row)); output.newLine(); output.flush();
                    Thread.sleep(1000);
                }
            } catch (Exception error) { throw new IllegalStateException(error); }
        }, "isolated-cache-probe");
        sampler.setDaemon(true); sampler.start();
    }
}
