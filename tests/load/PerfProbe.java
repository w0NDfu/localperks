package com.hmdp.perf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Copied into the disposable performance JAR only. */
@Component
public class PerfProbe {
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    @org.springframework.beans.factory.annotation.Qualifier("seckillAdmissionDataSource")
    private DataSource admission;
    private static final ConcurrentLinkedQueue<String> TIMINGS = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<String> HTTP = new ConcurrentLinkedQueue<>();
    private final DataSource source;

    public PerfProbe(DataSource source) {
        this.source = source;
    }

    public static void duration(String stage, long started) {
        TIMINGS.add(System.currentTimeMillis() + "," + Thread.currentThread().getName() + ","
                + stage + "," + (System.nanoTime() - started) / 1000);
    }

    public static void http(long enteredMs, String uid, long elapsedUs) {
        HTTP.add(enteredMs + "," + uid + "," + elapsedUs);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        Thread sampler = new Thread(this::sample, "isolated-perf-probe");
        sampler.setDaemon(true);
        sampler.start();
    }

    private void sample() {
        ObjectMapper json = new ObjectMapper();
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try (BufferedWriter metrics = Files.newBufferedWriter(Paths.get(System.getenv("SHOPLINK_PROBE_METRICS")), StandardCharsets.UTF_8);
             BufferedWriter timings = Files.newBufferedWriter(Paths.get(System.getenv("SHOPLINK_PROBE_TIMINGS")), StandardCharsets.UTF_8);
             BufferedWriter http = Files.newBufferedWriter(Paths.get(System.getenv("SHOPLINK_PROBE_TIMINGS")).resolveSibling("http-requests.csv"), StandardCharsets.UTF_8)) {
            timings.write("at_epoch_ms,thread,stage,duration_us\n");
            http.write("entered_ms,uid,service_us\n");
            while (!Thread.currentThread().isInterrupted()) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("at", System.currentTimeMillis());
                for (String sample; (sample = HTTP.poll()) != null;) {
                    http.write(sample + "\n");
                }
                http.flush();
                List<Map<String, Object>> pools = new ArrayList<>();
                for (ObjectName name : server.queryNames(new ObjectName("Tomcat:type=ThreadPool,*"), null)) {
                    Map<String, Object> values = new LinkedHashMap<>();
                    for (String attribute : new String[]{"maxThreads", "currentThreadsBusy", "currentThreadCount", "connectionCount", "keepAliveCount", "acceptCount", "maxConnections"}) {
                        try {
                            values.put(attribute, server.getAttribute(name, attribute));
                        } catch (Exception ignored) {
                            // Tomcat versions expose different attributes.
                        }
                    }
                    pools.add(values);
                }
                row.put("tomcat", pools);
                List<Map<String, Object>> processors = new ArrayList<>();
                for (ObjectName name : server.queryNames(new ObjectName("Tomcat:type=GlobalRequestProcessor,*"), null)) {
                    Map<String, Object> values = new LinkedHashMap<>();
                    for (String attribute : new String[]{"requestCount", "errorCount", "processingTime", "maxTime"}) {
                        values.put(attribute, server.getAttribute(name, attribute));
                    }
                    processors.add(values);
                }
                row.put("requests", processors);
                if (admission instanceof HikariDataSource) {
                    com.zaxxer.hikari.HikariPoolMXBean pool = ((HikariDataSource) admission).getHikariPoolMXBean();
                    if (pool != null) {
                        row.put("admissionActive", pool.getActiveConnections());
                        row.put("admissionPending", pool.getThreadsAwaitingConnection());
                        row.put("admissionTotal", pool.getTotalConnections());
                    }
                }
                if (source.isWrapperFor(HikariDataSource.class)) {
                    com.zaxxer.hikari.HikariPoolMXBean pool = source.unwrap(HikariDataSource.class).getHikariPoolMXBean();
                    if (pool != null) {
                        row.put("hikariActive", pool.getActiveConnections());
                        row.put("hikariIdle", pool.getIdleConnections());
                        row.put("hikariPending", pool.getThreadsAwaitingConnection());
                        row.put("hikariTotal", pool.getTotalConnections());
                    }
                }
                Map<String, Integer> states = new LinkedHashMap<>();
                for (ThreadInfo thread : ManagementFactory.getThreadMXBean().getThreadInfo(ManagementFactory.getThreadMXBean().getAllThreadIds())) {
                    if (thread != null && (thread.getThreadName().contains("consumer") || thread.getThreadName().startsWith("http-nio"))) {
                        String group = (thread.getThreadName().startsWith("http") ? "http:" : "consumer:") + thread.getThreadState();
                        states.merge(group, 1, Integer::sum);
                    }
                }
                row.put("threadStates", states);
                metrics.write(json.writeValueAsString(row));
                metrics.newLine();
                metrics.flush();
                String timing;
                while ((timing = TIMINGS.poll()) != null) {
                    timings.write(timing);
                    timings.newLine();
                }
                timings.flush();
                Thread.sleep(400);
            }
        } catch (Exception error) {
            error.printStackTrace();
        }
    }
}
