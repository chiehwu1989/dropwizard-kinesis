package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.MetricRegistry;
import io.codemonastery.dropwizard.kinesis.metric.KinesisMetricsProxy;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class KinesisClientBuilderTest {

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
        KinesisClientBuilder builder = new KinesisClientBuilder();
        client = builder.build(null, new NoCredentialsProvider(), "test-client");
    }

    @Test
    public void someMetrics() throws Exception {
        MetricRegistry metrics = new MetricRegistry();
        KinesisClientBuilder builder = new KinesisClientBuilder();
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
}
