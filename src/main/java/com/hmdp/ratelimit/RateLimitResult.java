package com.hmdp.ratelimit;

public class RateLimitResult {
    private final boolean allowed;
    private final long currentCount;
    private final long retryAfterMillis;
    private final int rejectedBucketIndex;

    public RateLimitResult(boolean allowed, long currentCount, long retryAfterMillis) {
        this(allowed, currentCount, retryAfterMillis, -1);
    }

    public RateLimitResult(boolean allowed, long currentCount, long retryAfterMillis, int rejectedBucketIndex) {
        this.allowed = allowed;
        this.currentCount = currentCount;
        this.retryAfterMillis = retryAfterMillis;
        this.rejectedBucketIndex = rejectedBucketIndex;
    }

    public int getRejectedBucketIndex() { return rejectedBucketIndex; }

    public boolean isAllowed() {
        return allowed;
    }

    public long getCurrentCount() {
        return currentCount;
    }

    public long getRetryAfterMillis() {
        return retryAfterMillis;
    }
}
