package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

public interface AcquireLimiter {

    double acquire(int numPermits);

    void update(int size, int rateExceededCount);

}
