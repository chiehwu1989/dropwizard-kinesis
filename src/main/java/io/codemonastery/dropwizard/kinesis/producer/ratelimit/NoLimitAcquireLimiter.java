package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

public class NoLimitAcquireLimiter implements AcquireLimiter {

    @Override
    public double acquire(int numPermits) {
        return 0.0;
    }

    @Override
    public void update(int size, int rateExceededCount) {
        //ignore
    }
}
