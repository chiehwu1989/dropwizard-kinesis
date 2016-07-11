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
        metrics.processorShutdown(null);
        metrics.processorStarted();
        //noinspection EmptyTryBlock
        try (AutoCloseable ignore = metrics.processTime()) {
        }
        metrics.processSuccess();
        metrics.unhandledException();
    }

    @Test
    public void decoded() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-decode-success").getCount()).isEqualTo(0);
        metrics.decoded();
        assertThat(metricsRegistry.meter("foo-decode-success").getCount()).isEqualTo(1);
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
    public void processTime() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.timer("foo-process").getCount()).isEqualTo(0);
        try(AutoCloseable ignored = metrics.processTime()){
            Thread.sleep(10);
        }
        assertThat(metricsRegistry.timer("foo-process").getCount()).isEqualTo(1);
    }

    @Test
    public void checkpointTime() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.timer("foo-checkpoint").getCount()).isEqualTo(0);
        try(AutoCloseable ignored = metrics.checkpointTime()){
            Thread.sleep(10);
        }
        assertThat(metricsRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
    }

    @Test
    public void checkpointFailure() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
        metrics.checkpointFailed();
        assertThat(metricsRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void noEventsNoFailure() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metrics.highFailureMetrics()).isEmpty();
    }

    @Test
    public void manySuccessFullEvents() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        for (int i = 0; i < 100; i++) {
            metrics.decoded();
            metrics.processSuccess();
        }
        assertThat(metrics.highFailureMetrics()).isEmpty();
    }

    @Test
    public void highEncodeFailure() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        for (int i = 0; i < 100; i++) {
            if(i % 2 == 0){
                metrics.decoded();
            }else{
                metrics.decodeFailure();
            }
            Thread.sleep(50);
        }
        assertThat(metrics.highFailureMetrics().size()).isEqualTo(1);
        assertThat(metrics.highFailureMetrics().get(0)).contains("% decode failure");
    }

    @Test
    public void highSendFailure() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        for (int i = 0; i < 100; i++) {
            if(i % 2 == 0){
                metrics.processSuccess();
            }else{
                metrics.processFailure();
            }
            Thread.sleep(50);
        }
        assertThat(metrics.highFailureMetrics().size()).isEqualTo(1);
        assertThat(metrics.highFailureMetrics().get(0)).contains("% process failure");
    }

    @Test
    public void highCheckpointFailure() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        for (int i = 0; i < 100; i++) {
            try(AutoCloseable ignore = metrics.checkpointTime()){
                if(i %2 == 0){
                    metrics.checkpointFailed();
                }
                Thread.sleep(50);
            }
        }
        assertThat(metrics.highFailureMetrics().size()).isEqualTo(1);
        assertThat(metrics.highFailureMetrics().get(0)).contains("% checkpoint failure");
    }

    @Test
    public void millisBehindLatest() throws Exception {
        RecordProcessorMetrics metrics = new RecordProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.getGauges().containsKey("foo-millis-behind-latest-123")).isFalse();
        metrics.millisBehindLatest("123", 1000);
        assertThat(metricsRegistry.getGauges().containsKey("foo-millis-behind-latest-123")).isTrue();
        metrics.processorShutdown("123");
        assertThat(metricsRegistry.getGauges().containsKey("foo-millis-behind-latest-123")).isFalse();
    }
}
