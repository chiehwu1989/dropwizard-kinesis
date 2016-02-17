package io.codemonastery.dropwizard.kinesis.consumer;

import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordProcessorMetricsTest {

    private MetricRegistry metricsRegistry;

    @Before
    public void setUp() throws Exception {
        this.metricsRegistry = new MetricRegistry();
    }

    @Test
    public void noOpWorksOk() throws Exception {
        RecordProcessorMetrics metrics = RecordProcessorMetrics.noOp();
        assertThat(metricsRegistry.getNames()).isEmpty();

        metrics.decodeFailure();
        metrics.processFailure();
        metrics.processorShutdown();
        metrics.processorStarted();
        //noinspection EmptyTryBlock
        try (AutoCloseable ignore = metrics.processTime()) {
        }
        metrics.processSuccess();
        metrics.unhandledException();
    }

    @Test
    public void decodeFailure() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        metrics.decodeFailure();
        assertThat(metricsRegistry.meter("foo-decode-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void failure() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-failure").getCount()).isEqualTo(0);
        metrics.processFailure();
        assertThat(metricsRegistry.meter("foo-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void success() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-success").getCount()).isEqualTo(0);
        metrics.processSuccess();
        assertThat(metricsRegistry.meter("foo-success").getCount()).isEqualTo(1);
    }

    @Test
    public void unhandledException() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-unhandled-exception").getCount()).isEqualTo(0);
        metrics.unhandledException();
        assertThat(metricsRegistry.meter("foo-unhandled-exception").getCount()).isEqualTo(1);
    }

    @Test
    public void time() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.timer("foo-process-time").getCount()).isEqualTo(0);
        try(AutoCloseable ignored = metrics.processTime()){
            Thread.sleep(10);
        }
        assertThat(metricsRegistry.timer("foo-process-time").getCount()).isEqualTo(1);
    }
}
