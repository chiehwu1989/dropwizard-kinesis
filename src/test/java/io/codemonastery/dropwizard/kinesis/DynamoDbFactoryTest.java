package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.util.StringInputStream;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.junit.After;
import org.junit.Test;

import javax.validation.Valid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class DynamoDbFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @Valid
        public DynamoDbFactory dynamoDb;

    }

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
        DynamoDbFactory builder = new DynamoDbFactory();
        client = builder.build(null, new NoCredentialsProvider(), "test-client");
    }

    @Test
    public void someMetrics() throws Exception {
        DynamoDbFactory builder = new DynamoDbFactory();
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

    @Test
    public void emptyConfigurationIsOk() throws Exception {
        FakeConfiguration configuration = ConfigurationFactories.make(FakeConfiguration.class)
                .build((s)->new StringInputStream("dynamoDb: {}"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.dynamoDb).isNotNull();
    }
}
