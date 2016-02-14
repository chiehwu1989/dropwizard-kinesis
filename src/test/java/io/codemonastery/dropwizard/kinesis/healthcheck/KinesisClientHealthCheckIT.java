package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.health.HealthCheck;
import io.codemonastery.dropwizard.kinesis.AmazonKinesisClientRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class KinesisClientHealthCheckIT {

    @ClassRule
    public static final AmazonKinesisClientRule CLIENT_RULE = new AmazonKinesisClientRule();
    private AmazonKinesis client;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        this.client = CLIENT_RULE.getClient();
    }

    @After
    public void tearDown() throws Exception {
        client = null;
    }

    @Test
    public void healthy() throws Exception {
        KinesisClientHealthCheck healthCheck = new KinesisClientHealthCheck(client);

        HealthCheck.Result result = healthCheck.check();
        assertNotNull(result);
        assertTrue(result.isHealthy());
    }
}
