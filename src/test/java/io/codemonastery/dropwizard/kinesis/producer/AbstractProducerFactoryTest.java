package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.MockitoAnnotations.initMocks;

public class AbstractProducerFactoryTest {

    @Mock
    private AmazonKinesis kinesis;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void inferEventEncoder() throws Exception {
        Environment env = new Environment("app", Jackson.newObjectMapper(), Validators.newValidator(), new MetricRegistry(), this.getClass().getClassLoader());

        AtomicReference<EventEncoder<String>> encoderRef = new AtomicReference<>(null);
        new AbstractProducerFactory<String>(){
            @Override
            public Producer<String> build(MetricRegistry metrics,
                                          HealthCheckRegistry healthChecks,
                                          LifecycleEnvironment lifecycle,
                                          AmazonKinesis kinesis,
                                          String name) {
                encoderRef.set(encoder);
                return null;
            }
        }.streamName("xyz").build(env, kinesis, "foo");
        assertThat(encoderRef.get()).isNotNull();
        assertThat(encoderRef.get().encode("abc")).isEqualTo(Jackson.newObjectMapper().writeValueAsBytes("abc"));
    }
}
