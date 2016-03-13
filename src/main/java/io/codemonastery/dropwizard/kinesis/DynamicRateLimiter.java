package io.codemonastery.dropwizard.kinesis;

import com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.TimeUnit;

public class DynamicRateLimiter {

    public static DynamicRateLimiter create(double initialPermitsPerSecond){
        return new DynamicRateLimiter(initialPermitsPerSecond, 2.0, initialPermitsPerSecond/1000.0);
    }

    private final double backoffDivisor;
    private final double moveForwardAddend;
    private final RateLimiter rateLimiter;

    DynamicRateLimiter(double initialPermitsPerSecond,
                              double backoffDivisor,
                              double moveForwardAddend) {
        this.backoffDivisor = backoffDivisor;
        this.moveForwardAddend = moveForwardAddend;
        this.rateLimiter = RateLimiter.create(initialPermitsPerSecond);
    }

    public double acquire() {
        return rateLimiter.acquire();
    }

    public double acquire(int permits) {
        return rateLimiter.acquire(permits);
    }

    public void backOff(){
        synchronized (this.rateLimiter){
            double newRate = this.rateLimiter.getRate() / backoffDivisor;
            this.rateLimiter.setRate(newRate);
        }
    }

    public void moveForward(){
        moveForward(1);
    }

    public void moveForward(int n){
        synchronized (this.rateLimiter){
            double newRate = this.rateLimiter.getRate() + (n * moveForwardAddend);
            this.rateLimiter.setRate(newRate);
        }
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) {
        return rateLimiter.tryAcquire(timeout, unit);
    }

    public boolean tryAcquire(int permits) {
        return rateLimiter.tryAcquire(permits);
    }

    public boolean tryAcquire() {
        return rateLimiter.tryAcquire();
    }

    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) {
        return rateLimiter.tryAcquire(permits, timeout, unit);
    }
}
