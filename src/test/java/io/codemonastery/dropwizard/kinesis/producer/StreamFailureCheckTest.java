package io.codemonastery.dropwizard.kinesis.producer;

import com.codahale.metrics.health.HealthCheck;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamFailureCheckTest {

    @Mock
    private HealthCheck stream;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void healthy() throws Exception {
        when(stream.execute()).thenReturn(HealthCheck.Result.healthy());
        ProducerMetrics metrics = new ProducerMetrics(null, "") {
            @Override
            public List<String> highFailureMetrics() {
                return new ArrayList<>();
            }
        };
        StreamFailureCheck healthCheck = new StreamFailureCheck(metrics, stream);
        assertThat(healthCheck.execute().isHealthy()).isTrue();
    }

    @Test
    public void unhealthyBecauseMetrics() throws Exception {
        when(stream.execute()).thenReturn(HealthCheck.Result.healthy());
        ProducerMetrics metrics = new ProducerMetrics(null, "") {
            @Override
            public List<String> highFailureMetrics() {
                return Collections.singletonList("A failure because of test");
            }
        };
        StreamFailureCheck healthCheck = new StreamFailureCheck(metrics,stream);
        HealthCheck.Result check = healthCheck.execute();
        assertThat(check.isHealthy()).isFalse();
        assertThat(check.getMessage()).contains("A failure because of test");
    }

    @Test
    public void unhealthyBecauseStreamHealth() throws Exception {
        when(stream.execute()).thenReturn(HealthCheck.Result.unhealthy("aaaaahhhhhh"));
        ProducerMetrics metrics = new ProducerMetrics(null, "") {
            @Override
            public List<String> highFailureMetrics() {
                return new ArrayList<>();
            }
        };
        StreamFailureCheck healthCheck = new StreamFailureCheck(metrics, stream);
        HealthCheck.Result check = healthCheck.execute();
        assertThat(check.isHealthy()).isFalse();
        assertThat(check.getMessage()).contains("aaaaahhhhhh");
    }
}
