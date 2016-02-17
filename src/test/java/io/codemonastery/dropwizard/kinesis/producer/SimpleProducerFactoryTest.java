package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.util.StringInputStream;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.ConfigurationFactories;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import io.dropwizard.setup.Environment;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.MockitoAnnotations.initMocks;

public class SimpleProducerFactoryTest {

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
    }

    @Test
    public void usesSimpleWhenSpecified() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("producer:\n  type: simple\n  streamName: xyz"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.producer.getStreamName()).isEqualTo("xyz");
        assertThat(configuration.producer).isInstanceOf(SimpleProducerFactory.class);
    }

    @Test
    public void allTheTings() throws Exception {
        String streamName = "xyz";
        Function<String, String> partitionkeyFunction = s -> s;
        EventEncoder<String> encoder = String::getBytes;

        SimpleProducerFactory<String> factory = (SimpleProducerFactory<String>)new SimpleProducerFactory<String>()
                .streamName(streamName)
                .partitionKeyFn(partitionkeyFunction)
                .encoder(encoder);
        assertThat(factory.getStreamName()).isEqualTo(streamName);
        assertThat(factory.getEncoder()).isSameAs(encoder);
        assertThat(factory.getPartitionKeyFn()).isSameAs(partitionkeyFunction);

        Environment env = new Environment("app", Jackson.newObjectMapper(), Validators.newValidator(), new MetricRegistry(), this.getClass().getClassLoader());
        assertThat(env.lifecycle().getManagedObjects().size()).isEqualTo(1);
        Producer<String> producer = factory.build(env, kinesis, "foo");
        assertThat(producer).isInstanceOf(SimpleProducer.class);

        assertThat(env.lifecycle().getManagedObjects().size()).isEqualTo(2);
        assertThat(env.metrics().getNames()).contains("foo-sent");
        assertThat(env.healthChecks().getNames()).contains("foo");
    }
}
