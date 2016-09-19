package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.Assertions;
import io.codemonastery.dropwizard.kinesis.ConfigurationFactories;
import io.codemonastery.dropwizard.kinesis.Environments;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import io.codemonastery.dropwizard.kinesis.producer.ratelimit.AcquireLimiterFactory;
import io.codemonastery.dropwizard.kinesis.producer.ratelimit.FixedAcquireLimiterFactory;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.util.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class BufferedProducerFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @NotNull
        @Valid
        public ProducerFactory<String> producer;

    }

    @Mock
    private AmazonKinesis kinesis;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        PutRecordsResult fakeResult = new PutRecordsResult()
                .withRecords(new ArrayList<>())
                .withFailedRecordCount(0);
        when(kinesis.putRecords(any())).thenReturn(fakeResult);
    }

    @Test
    public void usesBufferedByDefault() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("producer:\n  streamName: xyz"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.producer).isInstanceOf(BufferedProducerFactory.class);
        assertThat(configuration.producer.getStreamName()).isEqualTo("xyz");
    }

    @Test
    public void canConfigure() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("producer:\n  type: buffered\n  streamName: xyz"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.producer.getStreamName()).isEqualTo("xyz");
        assertThat(configuration.producer).isInstanceOf(BufferedProducerFactory.class);
    }

    @Test
    public void allTheThings() throws Exception {
        String streamName = "xyz";
        Function<String, String> partitionkeyFunction = s -> s;
        EventEncoder<String> encoder = String::getBytes;
        Duration flushPeriod = Duration.hours(1);
        int maxBufferSize = 111;
        AcquireLimiterFactory rateLimit = new FixedAcquireLimiterFactory();

        BufferedProducerFactory<String> factory = new BufferedProducerFactory<String>()
                .streamName(streamName)
                .partitionKeyFn(partitionkeyFunction)
                .encoder(encoder)
                .flushPeriod(flushPeriod)
                .maxBufferSize(maxBufferSize)
                .rateLimit(rateLimit);
        assertThat(factory.getStreamName()).isEqualTo(streamName);
        assertThat(factory.getEncoder()).isSameAs(encoder);
        assertThat(factory.getPartitionKeyFn()).isSameAs(partitionkeyFunction);
        assertThat(factory.getFlushPeriod()).isEqualTo(flushPeriod);
        assertThat(factory.getMaxBufferSize()).isEqualTo(maxBufferSize);
        assertThat(factory.getRateLimit()).isEqualTo(rateLimit);

        Environments.run("app", env->{
            assertThat(env.lifecycle().getManagedObjects().size()).isEqualTo(1);
            Producer<String> producer = factory.build(env, kinesis, "foo");
            assertThat(producer).isInstanceOf(BufferedProducer.class);

            assertThat(env.lifecycle().getManagedObjects().size()).isEqualTo(4);
            assertThat(env.metrics().getNames()).contains("foo-sent");
            assertThat(env.healthChecks().getNames()).contains("foo");
        });
    }

    @Test
    public void noLifecycleCanStillDeliver() throws Throwable {
        String streamName = "xyz";
        Function<String, String> partitionkeyFunction = s -> s;
        EventEncoder<String> encoder = String::getBytes;

        Duration flushPeriod = Duration.milliseconds(10);
        BufferedProducerFactory<String> factory = new BufferedProducerFactory<String>()
                .streamName(streamName)
                .partitionKeyFn(partitionkeyFunction)
                .encoder(encoder)
                .flushPeriod(flushPeriod);

        Producer<String> producer = factory.build(null, kinesis, "foo");
        verify(kinesis, never()).putRecords(any());
        producer.send("abc");
        try{
            Assertions.retry(10, flushPeriod, () -> verify(kinesis).putRecords(any()));
        }finally {
            producer.stop();
        }
    }
}
