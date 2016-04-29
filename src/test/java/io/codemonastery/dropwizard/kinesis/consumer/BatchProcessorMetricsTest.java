package io.codemonastery.dropwizard.kinesis.consumer;

import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BatchProcessorMetricsTest {

    private MetricRegistry metricsRegistry;

    @Before
    public void setUp() throws Exception {
        this.metricsRegistry = new MetricRegistry();
    }

    @Test
    public void noOpWorksOk() throws Exception {
        BatchProcessorMetrics metrics = BatchProcessorMetrics.noOp();
        assertThat(metricsRegistry.getNames()).isEmpty();

        metrics.decodeFailure();
        metrics.processFailure(10);
        metrics.processorShutdown();
        metrics.processorStarted();
        //noinspection EmptyTryBlock
        try (AutoCloseable ignore = metrics.processTime()) {
        }
        metrics.processSuccess(10);
        metrics.unhandledException();
    }

    @Test
    public void decoded() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-decode-success").getCount()).isEqualTo(0);
        metrics.decoded();
        assertThat(metricsRegistry.meter("foo-decode-success").getCount()).isEqualTo(1);
    }

    @Test
    public void decodeFailure() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        metrics.decodeFailure();
        assertThat(metricsRegistry.meter("foo-decode-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void failure() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-failure").getCount()).isEqualTo(0);
        assertThat(metricsRegistry.meter("foo-batch-failure").getCount()).isEqualTo(0);
        metrics.processFailure(10);
        assertThat(metricsRegistry.meter("foo-failure").getCount()).isEqualTo(10);
        assertThat(metricsRegistry.meter("foo-batch-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void success() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-success").getCount()).isEqualTo(0);
        assertThat(metricsRegistry.meter("foo-batch-success").getCount()).isEqualTo(0);
        metrics.processSuccess(10);
        assertThat(metricsRegistry.meter("foo-success").getCount()).isEqualTo(10);
        assertThat(metricsRegistry.meter("foo-batch-success").getCount()).isEqualTo(1);
    }

    @Test
    public void unhandledException() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-unhandled-exception").getCount()).isEqualTo(0);
        metrics.unhandledException();
        assertThat(metricsRegistry.meter("foo-unhandled-exception").getCount()).isEqualTo(1);
    }

    @Test
    public void processTime() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.timer("foo-process").getCount()).isEqualTo(0);
        try(AutoCloseable ignored = metrics.processTime()){
            Thread.sleep(10);
        }
        assertThat(metricsRegistry.timer("foo-process").getCount()).isEqualTo(1);
    }

    @Test
    public void checkpointTime() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.timer("foo-checkpoint").getCount()).isEqualTo(0);
        try(AutoCloseable ignored = metrics.checkpointTime()){
            Thread.sleep(10);
        }
        assertThat(metricsRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
    }

    @Test
    public void checkpointFailure() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        assertThat(metricsRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
        metrics.checkpointFailed();
        assertThat(metricsRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void noEventsNoFailure() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        assertThat(metrics.highFailureMetrics()).isEmpty();
    }

    @Test
    public void manySuccessFullEvents() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        for (int i = 0; i < 10; i++) {
            metrics.decoded();
            metrics.processSuccess(10);
        }
        assertThat(metrics.highFailureMetrics()).isEmpty();
    }

    @Test
    public void highEncodeFailure() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
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
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
        for (int i = 0; i < 50; i++) {
            if(i % 2 == 0){
                metrics.processSuccess(2);
            }else{
                metrics.processFailure(2);
            }
            Thread.sleep(100);
        }
        assertThat(metrics.highFailureMetrics().size()).isEqualTo(1);
        assertThat(metrics.highFailureMetrics().get(0)).contains("% process failure");
    }

    @Test
    public void highCheckpointFailure() throws Exception {
        BatchProcessorMetrics metrics = new BatchProcessorMetrics(metricsRegistry, "foo");
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
}
