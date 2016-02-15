package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import io.codemonastery.dropwizard.kinesis.rule.DynamoDbClientRule;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Ignore
public class DynamoDbClientHealthCheckIT {

    @ClassRule
    public static final DynamoDbClientRule CLIENT_RULE = new DynamoDbClientRule();

    @Test
    public void healthy() throws Exception {
        DynamoDbClientHealthCheck healthCheck = new DynamoDbClientHealthCheck(CLIENT_RULE.getClient());

        HealthCheck.Result result = healthCheck.check();
        assertNotNull(result);
        assertTrue(result.isHealthy());
    }

}
