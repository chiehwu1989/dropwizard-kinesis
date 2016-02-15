package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.ConfigurationFactories;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationFactory;
import org.junit.Test;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import static org.assertj.core.api.Assertions.assertThat;

public class BufferedProducerFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @NotNull
        @Valid
        public ProducerFactory<String> producer;

    }

    @Test
    public void canConfigure() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("producer:\n  type: buffered\n  streamName: xyz"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.producer.getStreamName()).isEqualTo("xyz");
        assertThat(configuration.producer).isInstanceOf(BufferedProducerFactory.class);
    }
}
