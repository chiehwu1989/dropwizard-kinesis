package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.util.StringInputStream;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.metric.ClientMetricsProxyFactory;
import io.codemonastery.dropwizard.kinesis.metric.KinesisMetricsProxy;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationValidationException;
import org.junit.After;
import org.junit.Test;

import javax.validation.Valid;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class KinesisFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @Valid
        public KinesisFactory kinesis;

    }

    private AmazonKinesis client;

    @After
    public void tearDown() throws Exception {
        if(client != null){
            client.shutdown();
            client = null;
        }
    }

    @Test
    public void nullEnvironment() throws Exception {
        KinesisFactory builder = new KinesisFactory();
        client = builder.build(null, new NoCredentialsProvider(), "test-client");
    }

    @Test
    public void someMetrics() throws Exception {
        MetricRegistry metrics = new MetricRegistry();
        KinesisFactory builder = new KinesisFactory();
        client = builder.build(metrics, null, null, new NoCredentialsProvider(), "test-client");
        assertThat(client).isInstanceOf(KinesisMetricsProxy.class);
        try{
            client.listStreams();
            fail("was expecting credentials problem");
        }catch (AmazonServiceException e){
            //ignore
        }
        assertThat(metrics.timer("test-client-list-streams").getCount()).isEqualTo(1L);
    }

    @Test(expected = ConfigurationValidationException.class)
    public void regionIsRequired() throws Exception {
        ConfigurationFactories.make(FakeConfiguration.class)
                .build((s)->new StringInputStream("kinesis: {}"), "");

    }

    @Test
    public void regionOnlyIsOk() throws Exception {
        FakeConfiguration configuration = ConfigurationFactories.make(FakeConfiguration.class)
                .build((s)->new StringInputStream("kinesis:\n  region: US_WEST_2"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.kinesis).isNotNull();
        assertThat(configuration.kinesis.getRegion()).isEqualTo(Regions.US_WEST_2);
    }

    @Test
    public void allTheThings() throws Exception {
        Environments.run("app", env->{
            Regions region = Regions.CN_NORTH_1;
            ClientMetricsProxyFactory<AmazonKinesis> metricsProxy = KinesisMetricsProxy::new;
            JacksonClientConfiguration client = new JacksonClientConfiguration();

            KinesisFactory factory = new KinesisFactory().region(region)
                    .metricsProxy(metricsProxy)
                    .client(client);

            assertThat(factory.getRegion()).isEqualTo(region);
            assertThat(factory.getMetricsProxy()).isSameAs(metricsProxy);
            assertThat(factory.getClient()).isSameAs(client);

            assertThat(env.lifecycle().getManagedObjects().size()).isEqualTo(1);
            AmazonKinesis kinesis = factory.build(env, new NoCredentialsProvider(), "foo");
            assertThat(kinesis).isInstanceOf(KinesisMetricsProxy.class);

            assertThat(env.lifecycle().getManagedObjects().size()).isEqualTo(2);
            assertThat(env.metrics().getNames()).contains("foo-list-streams");
            assertThat(env.healthChecks().getNames().contains("foo"));
        });
    }
}
