package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import io.codemonastery.dropwizard.kinesis.Assertions;
import io.dropwizard.util.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import static org.junit.Assert.assertEquals;

public class DynamicAcquireLimiterTest {

    @Rule
    public final TestRule RETRY_BECAUSE_SLEEPS = (statement, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            Assertions.retry(3, Duration.milliseconds(0), statement::evaluate);
        }
    };

    @Test
    public void allPermits() throws Exception {
        DynamicAcquireLimiter rateLimiter = new DynamicAcquireLimiter(100, 2.0, 1.0);
        assertEquals(0.0, rateLimiter.acquire(100), 0.0);
        for (int i = 0; i < 3; i++) {
            assertEquals(1.0, rateLimiter.acquire(100), 0.1);
        }
    }

    @Test
    public void somePermits() throws Exception {
        DynamicAcquireLimiter rateLimiter = new DynamicAcquireLimiter(100, 2.0, 1.0);
        assertEquals(0.0, rateLimiter.acquire(50), 0.0);
        for (int i = 0; i < 3; i++) {
            assertEquals(0.5, rateLimiter.acquire(50), 0.1);
        }
    }

    @Test
    public void backoff() throws Exception {
        DynamicAcquireLimiter rateLimiter = new DynamicAcquireLimiter(1000.0, 2.0, 1.0);
        rateLimiter.backOff();
        assertEquals(0.0, rateLimiter.acquire(500), 0.0);
        for (int i = 0; i < 3; i++) {
            assertEquals(1.0, rateLimiter.acquire(500), 0.1);
        }
    }

    @Test
    public void moveforward() throws Exception {
        DynamicAcquireLimiter rateLimiter = new DynamicAcquireLimiter(100.0, 2.0, 1.0);
        for (int i = 0; i < 100.0; i++) {
            rateLimiter.moveForward();
        }
        assertEquals(0.0, rateLimiter.acquire(100), 0.0);
        for (int i = 0; i < 3; i++) {
            assertEquals(0.5, rateLimiter.acquire(100), 0.1);
        }
    }

    @Test
    public void backoffThroughUpdate() throws Exception {
        DynamicAcquireLimiter rateLimiter = new DynamicAcquireLimiter(1000.0, 2.0, 1.0);
        rateLimiter.update(10, 1);
        assertEquals(0.0, rateLimiter.acquire(500), 0.0);
        for (int i = 0; i < 3; i++) {
            assertEquals(1.0, rateLimiter.acquire(500), 0.1);
        }
    }

    @Test
    public void moveforwardThroughUpdate() throws Exception {
        DynamicAcquireLimiter rateLimiter = new DynamicAcquireLimiter(100.0, 2.0, 1.0);
        for (int i = 0; i < 100.0; i++) {
            rateLimiter.update(1, 0);
        }
        assertEquals(0.0, rateLimiter.acquire(100), 0.0);
        for (int i = 0; i < 3; i++) {
            assertEquals(0.5, rateLimiter.acquire(100), 0.1);
        }
    }
}
