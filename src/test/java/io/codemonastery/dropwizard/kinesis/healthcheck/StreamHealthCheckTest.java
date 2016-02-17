package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.codahale.metrics.health.HealthCheck;
import io.codemonastery.dropwizard.kinesis.KinesisResults;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamHealthCheckTest {

    public static final String STREAM_NAME = "test-stream";
    @Mock
    private AmazonKinesis kinesis;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void unhealthyBecauseStreamDoesNotExist() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenThrow(new ResourceNotFoundException("ahhh real monsters"));
        StreamHealthCheck healthCheck = new StreamHealthCheck(kinesis, "test-stream");
        HealthCheck.Result check = healthCheck.execute();
        assertThat(check.isHealthy()).isFalse();
        assertThat(check.getMessage()).contains("does not exist");
    }

    @Test
    public void unhealthyBecauseStreamIsNotActive() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenReturn(KinesisResults.creatingStream(STREAM_NAME));
        StreamHealthCheck healthCheck = new StreamHealthCheck(kinesis, "test-stream");
        HealthCheck.Result check = healthCheck.execute();
        assertThat(check.isHealthy()).isFalse();
        assertThat(check.getMessage()).contains("is not active");
    }

    @Test
    public void unhealthyBecauseUnhandledException() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenThrow(new RuntimeException("ahhh real monsters"));
        StreamHealthCheck healthCheck = new StreamHealthCheck(kinesis, "test-stream");
        HealthCheck.Result check = healthCheck.execute();
        assertThat(check.isHealthy()).isFalse();
        assertThat(check.getMessage()).contains("could not check status of stream");
    }

}
