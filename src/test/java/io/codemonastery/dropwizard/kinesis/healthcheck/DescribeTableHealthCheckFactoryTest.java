package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.codahale.metrics.health.HealthCheck;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class DescribeTableHealthCheckFactoryTest {

    @Mock
    private AmazonDynamoDB dynamoDb;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void tableDoesNotNeedToExist() throws Exception {
        when(dynamoDb.describeTable(anyString())).thenThrow(new ResourceNotFoundException("table does not exist"));
        HealthCheck healthCheck = new DescribeTableHealthCheckFactory()
                .tableName("test")
                .tableMustExist(false)
                .build(dynamoDb);

        assertThat(healthCheck.execute().isHealthy()).isTrue();
    }

    @Test
     public void tableNeedsToExist() throws Exception {
        when(dynamoDb.describeTable(anyString())).thenThrow(new ResourceNotFoundException("table does not exist"));
        HealthCheck healthCheck = new DescribeTableHealthCheckFactory()
                .tableName("test")
                .tableMustExist(true)
                .build(dynamoDb);

        assertThat(healthCheck.execute().isHealthy()).isFalse();
    }

    @Test
     public void unhealthyBecauseOtherException() throws Exception {
        when(dynamoDb.describeTable(anyString())).thenThrow(new RuntimeException("someone dropped my packet"));
        HealthCheck healthCheck = new DescribeTableHealthCheckFactory()
                .tableName("test")
                .tableMustExist(false)
                .build(dynamoDb);

        HealthCheck.Result result = healthCheck.execute();
        assertThat(result.isHealthy()).isFalse();
        assertThat(result.getMessage().contains("packet"));
    }
}
