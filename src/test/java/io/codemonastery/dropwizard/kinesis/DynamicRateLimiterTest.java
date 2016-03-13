package io.codemonastery.dropwizard.kinesis;

import io.dropwizard.util.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.model.Statement;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@RunWith(Enclosed.class)
public class DynamicRateLimiterTest {

    @Rule
    public final TestRule RETRY_BECAUSE_SLEEPS = (statement, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            Assertions.retry(3, Duration.milliseconds(0), statement::evaluate);
        }
    };

    public static class Acquire {

        @Test
        public void allPermits() throws Exception {
            DynamicRateLimiter rateLimiter = new DynamicRateLimiter(100, 2.0, 1.0);
            assertEquals(0.0, rateLimiter.acquire(100), 0.0);
            for (int i = 0; i < 3; i++) {
                assertEquals(1.0, rateLimiter.acquire(100), 0.1);
            }
        }

        @Test
        public void somePermits() throws Exception {
            DynamicRateLimiter rateLimiter = new DynamicRateLimiter(100, 2.0, 1.0);
            assertEquals(0.0, rateLimiter.acquire(50), 0.0);
            for (int i = 0; i < 3; i++) {
                assertEquals(0.5, rateLimiter.acquire(50), 0.1);
            }
        }
    }

    public static class TryAcquire {

        @Test
        public void allPermits() throws Exception {
            DynamicRateLimiter rateLimiter = new DynamicRateLimiter(100, 2.0, 1.0);
            assertTrue(rateLimiter.tryAcquire(100));
            assertFalse(rateLimiter.tryAcquire(100));
        }

        @Test
        public void somePermits() throws Exception {
            DynamicRateLimiter rateLimiter = new DynamicRateLimiter(100, 2.0, 1.0);
            assertTrue(rateLimiter.tryAcquire(500));
            assertFalse(rateLimiter.tryAcquire(500));
        }
    }

    public static class TryAcquireTimeout {

        @Test
        public void allPermits() throws Exception {
            DynamicRateLimiter rateLimiter = new DynamicRateLimiter(1000.0, 2.0, 1.0);
            assertTrue(rateLimiter.tryAcquire(1000, 0, TimeUnit.MILLISECONDS));
            for (int i = 0; i < 3; i++) {
                assertFalse(rateLimiter.tryAcquire(1000, 0, TimeUnit.MILLISECONDS));
                assertTrue(rateLimiter.tryAcquire(1000, 1, TimeUnit.SECONDS));
            }
        }

        @Test
        public void somePermits() throws Exception {
            DynamicRateLimiter rateLimiter = new DynamicRateLimiter(1000.0, 2.0, 1.0);
            assertTrue(rateLimiter.tryAcquire(500, 0, TimeUnit.MILLISECONDS));
            for (int i = 0; i < 3; i++) {
                assertFalse(rateLimiter.tryAcquire(500, 0, TimeUnit.MILLISECONDS));
                assertTrue(rateLimiter.tryAcquire(500, 500, TimeUnit.MILLISECONDS));
            }
        }
    }

    public static class DynamicRate {

        @Test
        public void backoff() throws Exception {
            DynamicRateLimiter rateLimiter = new DynamicRateLimiter(1000.0, 2.0, 1.0);
            rateLimiter.backOff();
            assertEquals(0.0, rateLimiter.acquire(500), 0.0);
            for (int i = 0; i < 3; i++) {
                assertEquals(1.0, rateLimiter.acquire(500), 0.1);
            }
        }

        @Test
        public void moveforward() throws Exception {
            DynamicRateLimiter rateLimiter = new DynamicRateLimiter(100.0, 2.0, 1.0);
            for (int i = 0; i < 100.0; i++) {
                rateLimiter.moveForward();
            }
            assertEquals(0.0, rateLimiter.acquire(100), 0.0);
            for (int i = 0; i < 3; i++) {
                assertEquals(0.5, rateLimiter.acquire(100), 0.1);
            }
        }
    }
}
