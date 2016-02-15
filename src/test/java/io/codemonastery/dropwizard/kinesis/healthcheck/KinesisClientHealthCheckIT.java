package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import io.codemonastery.dropwizard.kinesis.rule.KinesisClientRule;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


@Ignore
public class KinesisClientHealthCheckIT {

    @ClassRule
    public static final KinesisClientRule CLIENT_RULE = new KinesisClientRule();

    @Test
    public void healthy() throws Exception {
        KinesisClientHealthCheck healthCheck = new KinesisClientHealthCheck(CLIENT_RULE.getClient());

        HealthCheck.Result result = healthCheck.check();
        assertNotNull(result);
        assertTrue(result.isHealthy());
    }
}
