package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.health.HealthCheck;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class KinesisClientHealthCheckTest {

    @Mock
    private AmazonKinesis client;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void healthy() throws Exception {
        KinesisClientHealthCheck healthCheck = new KinesisClientHealthCheck(client);

        HealthCheck.Result result = healthCheck.execute();
        assertNotNull(result);
        assertTrue(result.isHealthy());
    }

    @Test
    public void unhealthy() throws Exception {
        when(client.listStreams()).thenThrow(new RuntimeException("exception from test"));

        KinesisClientHealthCheck healthCheck = new KinesisClientHealthCheck(client);

        HealthCheck.Result result = healthCheck.execute();
        assertNotNull(result);
        assertFalse(result.isHealthy());
        assertNotNull(result.getMessage());
        assertTrue(result.getMessage().contains("exception from test"));
    }
}
