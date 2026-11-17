package com.hmdp.ops;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import java.util.*;

/** Bounded structured observations. Never stores tokens, message bodies or user identities. */
@Slf4j
@Component
public class OpsJournal {
    private final Deque<Map<String, Object>> events = new ArrayDeque<>();
    private final long recordingSince = System.currentTimeMillis();
    private long sequence;
    private long evicted;

    public synchronized void record(String type, Object... pairs) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("seq", ++sequence);
        event.put("at", System.currentTimeMillis());
        event.put("type", type);
        for (int i = 0; i + 1 < pairs.length; i += 2) event.put(String.valueOf(pairs[i]), pairs[i + 1]);
        events.addLast(event);
        if (events.size() > 5000) { events.removeFirst(); evicted++; }
        log.info("OPS_EVENT {}", event);
    }

    public synchronized Map<String, Object> window(long start, long end, Long voucher) {
        List<Map<String, Object>> selected = new ArrayList<>();
        int matching = 0;
        for (Map<String, Object> event : events) {
            long at = ((Number) event.get("at")).longValue();
            if (at < start || at > end) continue;
            if (voucher != null && event.containsKey("voucherId") && !voucher.toString().equals(String.valueOf(event.get("voucherId")))) continue;
            matching++;
            if (selected.size() < 500) selected.add(new LinkedHashMap<>(event));
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("events", selected); result.put("recordingSince", recordingSince);
        result.put("matching", matching); result.put("truncated", matching > selected.size());
        result.put("evicted", evicted);
        result.put("coverageComplete", start >= recordingSince && (evicted == 0 || (!events.isEmpty() && start >= ((Number) events.peekFirst().get("at")).longValue())) && matching <= 500);
        return result;
    }
}
