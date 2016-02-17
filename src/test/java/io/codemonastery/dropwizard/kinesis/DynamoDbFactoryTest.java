package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.util.StringInputStream;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.metric.ClientMetricsProxyFactory;
import io.codemonastery.dropwizard.kinesis.metric.DynamoDbMetricsProxy;
import io.dropwizard.Configuration;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import io.dropwizard.setup.Environment;
import org.junit.After;
import org.junit.Test;

import javax.validation.Valid;

import static org.assertj.core.api.Assertions.assertThat;

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
    public void emptyConfigurationIsOk() throws Exception {
        FakeConfiguration configuration = ConfigurationFactories.make(FakeConfiguration.class)
                .build((s)->new StringInputStream("dynamoDb: {}"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.dynamoDb).isNotNull();
    }

    @Test
    public void allTheThings() throws Exception {
        JacksonClientConfiguration client = new  JacksonClientConfiguration();
        Regions region = Regions.US_WEST_2;
        ClientMetricsProxyFactory<AmazonDynamoDB> metricsProxyFactory = DynamoDbMetricsProxy::new;

        DynamoDbFactory factory = new DynamoDbFactory()
                .client(client)
                .region(region)
                .metricsProxy(metricsProxyFactory);

        assertThat(factory.getClient()).isSameAs(client);
        assertThat(factory.getRegion()).isSameAs(region);
        assertThat(factory.getMetricsProxyFactory()).isSameAs(metricsProxyFactory);

        Environment environment = new Environment("app", Jackson.newObjectMapper(), Validators.newValidator(), new MetricRegistry(), this.getClass().getClassLoader());
        assertThat(environment.lifecycle().getManagedObjects().size()).isEqualTo(1);
        AmazonDynamoDB dynamoDB = factory.build(environment, new NoCredentialsProvider(), "foo");

        assertThat(dynamoDB).isInstanceOf(DynamoDbMetricsProxy.class);
        assertThat(environment.healthChecks().getNames().contains("foo"));
        assertThat(environment.lifecycle().getManagedObjects().size()).isEqualTo(2);
    }
}
