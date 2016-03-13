package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import io.codemonastery.dropwizard.kinesis.Assertions;
import io.dropwizard.util.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import static org.junit.Assert.assertEquals;

public class FixedAcquireLimiterTest {

    @Rule
    public final TestRule RETRY_BECAUSE_SLEEPS = (statement, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            Assertions.retry(3, Duration.milliseconds(0), statement::evaluate);
        }
    };

    @Test
    public void allPermits() throws Exception {
        FixedAcquireLimiter rateLimiter = new FixedAcquireLimiter(100);
        rateLimiter.update(0, 0); // test coverage
        assertEquals(0.0, rateLimiter.acquire(100), 0.0);
        for (int i = 0; i < 3; i++) {
            assertEquals(1.0, rateLimiter.acquire(100), 0.1);
        }
    }

    @Test
    public void somePermits() throws Exception {
        FixedAcquireLimiter rateLimiter = new FixedAcquireLimiter(100);
        assertEquals(0.0, rateLimiter.acquire(50), 0.0);
        for (int i = 0; i < 3; i++) {
            assertEquals(0.5, rateLimiter.acquire(50), 0.1);
        }
    }
}
