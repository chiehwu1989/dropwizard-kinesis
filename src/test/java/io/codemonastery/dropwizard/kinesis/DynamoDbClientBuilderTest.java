package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.codahale.metrics.MetricRegistry;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class DynamoDbClientBuilderTest {

    private AmazonDynamoDB client;

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.shutdown();
            client = null;
        }
    }

    @Test
    public void nullEnvironment() throws Exception {
        DynamoDbClientBuilder builder = new DynamoDbClientBuilder();
        client = builder.build(null, new NoCredentialsProvider(), "test-client");
    }

    @Test
    public void someMetrics() throws Exception {
        DynamoDbClientBuilder builder = new DynamoDbClientBuilder();
        MetricRegistry metrics = new MetricRegistry();
        client = builder.build(metrics, null, null, new NoCredentialsProvider(), "test-client");
        try {
            client.listTables();
            fail("was supposed to throw exception because credentials");
        } catch (AmazonServiceException e) {
            //ignore
        }
        assertThat(metrics.timer("test-client-list-tables").getCount()).isEqualTo(1);
    }
}