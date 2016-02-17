package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.codahale.metrics.health.HealthCheck;
import io.codemonastery.dropwizard.kinesis.KinesisResults;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class ProducerHealthCheckTest {

    public static final String STREAM_NAME = "test-stream";
    @Mock
    private AmazonKinesis kinesis;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void healthy() throws Exception {
        when(kinesis.describeStream(STREAM_NAME)).thenReturn(KinesisResults.activeStream("test-stream"));
        ProducerMetrics metrics = new ProducerMetrics(null, "") {
            @Override
            public List<String> highFailureMetrics() {
                return new ArrayList<>();
            }
        };
        ProducerHealthCheck healthCheck = new ProducerHealthCheck(metrics, kinesis, "test-stream");
        assertThat(healthCheck.check().isHealthy()).isTrue();
    }

    @Test
    public void unhealthyBecauseMetrics() throws Exception {
        when(kinesis.describeStream(STREAM_NAME)).thenReturn(KinesisResults.activeStream("test-stream"));
        ProducerMetrics metrics = new ProducerMetrics(null, "") {
            @Override
            public List<String> highFailureMetrics() {
                return Collections.singletonList("A failure because of test");
            }
        };
        ProducerHealthCheck healthCheck = new ProducerHealthCheck(metrics, kinesis, "test-stream");
        HealthCheck.Result check = healthCheck.check();
        assertThat(check.isHealthy()).isFalse();
        assertThat(check.getMessage()).contains("A failure because of test");
    }

    @Test
    public void unhealthyBecauseStreamDoesNotExist() throws Exception {
        when(kinesis.describeStream(STREAM_NAME)).thenThrow(new ResourceNotFoundException(STREAM_NAME + " does not exist"));
        ProducerMetrics metrics = new ProducerMetrics(null, "") {
            @Override
            public List<String> highFailureMetrics() {
                return new ArrayList<>();
            }
        };
        ProducerHealthCheck healthCheck = new ProducerHealthCheck(metrics, kinesis, "test-stream");
        HealthCheck.Result check = healthCheck.check();
        assertThat(check.isHealthy()).isFalse();
        assertThat(check.getMessage()).contains("does not exist");
    }

    @Test
    public void unhealthyBecauseStreamIsNotActive() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenReturn(KinesisResults.creatingStream(STREAM_NAME));
        ProducerMetrics metrics = new ProducerMetrics(null, "") {
            @Override
            public List<String> highFailureMetrics() {
                return new ArrayList<>();
            }
        };
        ProducerHealthCheck healthCheck = new ProducerHealthCheck(metrics, kinesis, "test-stream");
        HealthCheck.Result check = healthCheck.check();
        assertThat(check.isHealthy()).isFalse();
        assertThat(check.getMessage()).contains("is not active");
    }
}
