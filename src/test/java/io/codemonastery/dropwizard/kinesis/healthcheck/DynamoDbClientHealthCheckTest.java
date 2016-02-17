package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.codahale.metrics.health.HealthCheck;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DynamoDbClientHealthCheckTest {

    @Mock
    private AmazonDynamoDB client;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void healthy() throws Exception {
        DynamoDbClientHealthCheck healthCheck = new DynamoDbClientHealthCheck(client);

        HealthCheck.Result result = healthCheck.check();
        assertNotNull(result);
        assertTrue(result.isHealthy());
    }

    @Test
    public void unhealthy() throws Exception {
        when(client.listTables()).thenThrow(new RuntimeException("exception from test"));

        DynamoDbClientHealthCheck healthCheck = new DynamoDbClientHealthCheck(client);

        HealthCheck.Result result = healthCheck.check();
        assertNotNull(result);
        assertFalse(result.isHealthy());
        assertNotNull(result.getMessage());
        assertTrue(result.getMessage().contains("exception from test"));
    }

}
