package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import com.google.common.util.concurrent.RateLimiter;

public class FixedAcquireLimiter implements AcquireLimiter {

    private final RateLimiter rateLimiter;

    public FixedAcquireLimiter(double rateLimit) {
        this.rateLimiter = RateLimiter.create(rateLimit);
    }

    @Override
    public double acquire(int numPermits) {
        return rateLimiter.acquire(numPermits);
    }

    @Override
    public void update(int size, int rateExceededCount) {
        //nothing to do, fixed rate limit
    }
}
