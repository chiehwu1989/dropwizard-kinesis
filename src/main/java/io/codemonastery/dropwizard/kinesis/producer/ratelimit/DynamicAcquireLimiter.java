package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import com.google.common.util.concurrent.RateLimiter;

public class DynamicAcquireLimiter implements AcquireLimiter {

    public static DynamicAcquireLimiter create(double initialPermitsPerSecond) {
        return create(initialPermitsPerSecond, 2.0, 1.0 / 60.0);
    }

    public static DynamicAcquireLimiter create(double initialPermitsPerSecond,
                                            double backoffDivisor,
                                            double moveForwardAddend) {
        return new DynamicAcquireLimiter(initialPermitsPerSecond,
                backoffDivisor,
                moveForwardAddend);
    }

    private final double backoffDivisor;
    private final double moveForwardAddend;
    private final RateLimiter rateLimiter;

    DynamicAcquireLimiter(double initialPermitsPerSecond,
                          double backoffDivisor,
                          double moveForwardAddend) {
        this.backoffDivisor = backoffDivisor;
        this.moveForwardAddend = moveForwardAddend;
        this.rateLimiter = RateLimiter.create(initialPermitsPerSecond);
    }

    public double acquire(int permits) {
        return rateLimiter.acquire(permits);
    }

    @Override
    public void update(int size, int rateExceededCount) {
        if(rateExceededCount > 0){
            backOff();
        }else{
            moveForward();
        }
    }

    public void backOff() {
        synchronized (this.rateLimiter) {
            double newRate = this.rateLimiter.getRate() / backoffDivisor;
            this.rateLimiter.setRate(newRate);
        }
    }

    public void moveForward() {
        synchronized (this.rateLimiter) {
            double newRate = this.rateLimiter.getRate() + moveForwardAddend;
            this.rateLimiter.setRate(newRate);
        }
    }

}
