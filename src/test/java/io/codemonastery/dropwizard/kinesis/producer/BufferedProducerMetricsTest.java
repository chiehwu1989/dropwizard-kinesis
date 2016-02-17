package io.codemonastery.dropwizard.kinesis.producer;

import com.codahale.metrics.MetricRegistry;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BufferedProducerMetricsTest {

    @Test
    public void bufferPut() throws Exception {
        MetricRegistry metricRegistry = new MetricRegistry();
        BufferedProducerMetrics metrics = new BufferedProducerMetrics(metricRegistry, "foo");

        assertThat(metricRegistry.meter("foo-buffer-put").getCount()).isEqualTo(0);
        assertThat(metricRegistry.counter("foo-buffer-size").getCount()).isEqualTo(0);

        metrics.bufferPut(10);

        assertThat(metricRegistry.meter("foo-buffer-put").getCount()).isEqualTo(10);
        assertThat(metricRegistry.counter("foo-buffer-size").getCount()).isEqualTo(10);

        metrics.bufferRemove(9);
        assertThat(metricRegistry.counter("foo-buffer-size").getCount()).isEqualTo(1);
    }
}
