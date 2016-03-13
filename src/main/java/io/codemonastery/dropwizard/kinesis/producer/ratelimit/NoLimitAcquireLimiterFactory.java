package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

public class NoLimitAcquireLimiterFactory implements AcquireLimiterFactory {

    @Override
    public NoLimitAcquireLimiter build() {
        return new NoLimitAcquireLimiter();
    }
}
