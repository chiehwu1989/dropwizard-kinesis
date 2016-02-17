package io.codemonastery.dropwizard.kinesis.producer;

import com.codahale.metrics.MetricRegistry;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ProducerMetricsTest {

    @Test
    public void noOp() throws Exception {
        ProducerMetrics metrics = ProducerMetrics.noOp();
        try(AutoCloseable ignore = metrics.time()){
            Thread.sleep(50);
        }
    }

    @Test
    public void encodeSuccess() throws Exception {
        MetricRegistry metricsRegistry = new MetricRegistry();
        ProducerMetrics metrics = new ProducerMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-encode-success").getCount()).isEqualTo(0);
        metrics.encoded();
        assertThat(metricsRegistry.meter("foo-encode-success").getCount()).isEqualTo(1);
    }

    @Test
    public void encodeFailure() throws Exception {
        MetricRegistry metricsRegistry = new MetricRegistry();
        ProducerMetrics metrics = new ProducerMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-encode-failure").getCount()).isEqualTo(0);
        metrics.encodeFailed();
        assertThat(metricsRegistry.meter("foo-encode-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void partitionKeySuccess() throws Exception {
        MetricRegistry metricsRegistry = new MetricRegistry();
        ProducerMetrics metrics = new ProducerMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-partition-key-success").getCount()).isEqualTo(0);
        metrics.partitionKeyed();
        assertThat(metricsRegistry.meter("foo-partition-key-success").getCount()).isEqualTo(1);
    }

    @Test
    public void partitionKeyFailure() throws Exception {
        MetricRegistry metricsRegistry = new MetricRegistry();
        ProducerMetrics metrics = new ProducerMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-partition-key-failure").getCount()).isEqualTo(0);
        metrics.partitionKeyFailed();
        assertThat(metricsRegistry.meter("foo-partition-key-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void sent() throws Exception {
        MetricRegistry metricsRegistry = new MetricRegistry();
        ProducerMetrics metrics = new ProducerMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-sent").getCount()).isEqualTo(0);
        metrics.sent(1, 0);
        assertThat(metricsRegistry.meter("foo-sent").getCount()).isEqualTo(1);
    }

    @Test
    public void failed() throws Exception {
        MetricRegistry metricsRegistry = new MetricRegistry();
        ProducerMetrics metrics = new ProducerMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-failed").getCount()).isEqualTo(0);
        metrics.sent(0, 1);
        assertThat(metricsRegistry.meter("foo-failed").getCount()).isEqualTo(1);
    }

    @Test
    public void putRecordsTimer() throws Exception {
        MetricRegistry metricsRegistry = new MetricRegistry();
        ProducerMetrics metrics = new ProducerMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.timer("foo-put-records-time").getCount()).isEqualTo(0);
        try(AutoCloseable ignored = metrics.time()){
            Thread.sleep(50);
        }
        assertThat(metricsRegistry.timer("foo-put-records-time").getCount()).isEqualTo(1);
    }

    @Test
    public void noEventsNoFailure() throws Exception {
        ProducerMetrics metrics = new ProducerMetrics(new MetricRegistry(), "foo");
        assertThat(metrics.highFailureMetrics()).isEmpty();
    }

    @Test
    public void manySuccessFullEvents() throws Exception {
        ProducerMetrics metrics = new ProducerMetrics(new MetricRegistry(), "foo");
        for (int i = 0; i < 100; i++) {
            metrics.encoded();
            metrics.sent(i+1, 0);
        }
        assertThat(metrics.highFailureMetrics()).isEmpty();
    }

    @Test
    public void highEncodeFailure() throws Exception {
        MetricRegistry registry = new MetricRegistry();
        ProducerMetrics metrics = new ProducerMetrics(registry, "foo");
        for (int i = 0; i < 100; i++) {
            if(i % 2 == 0){
                metrics.encoded();
            }else{
                metrics.encodeFailed();
            }
            Thread.sleep(50);
        }
        assertThat(metrics.highFailureMetrics().size()).isEqualTo(1);
        assertThat(metrics.highFailureMetrics().get(0)).contains("% encode failure");
    }

    @Test
    public void highPartitionKeyFailure() throws Exception {
        MetricRegistry registry = new MetricRegistry();
        ProducerMetrics metrics = new ProducerMetrics(registry, "foo");
        for (int i = 0; i < 100; i++) {
            if(i % 2 == 0){
                metrics.partitionKeyed();
            }else{
                metrics.partitionKeyFailed();
            }
            Thread.sleep(50);
        }
        assertThat(metrics.highFailureMetrics().size()).isEqualTo(1);
        assertThat(metrics.highFailureMetrics().get(0)).contains("% partition key failure");
    }

    @Test
    public void highSendFailure() throws Exception {
        MetricRegistry registry = new MetricRegistry();
        ProducerMetrics metrics = new ProducerMetrics(registry, "foo");
        for (int i = 0; i < 100; i++) {
            metrics.sent(10-5, 5);
            Thread.sleep(50);
        }
        assertThat(metrics.highFailureMetrics().size()).isEqualTo(1);
        assertThat(metrics.highFailureMetrics().get(0)).contains("% send failure");
    }
}
