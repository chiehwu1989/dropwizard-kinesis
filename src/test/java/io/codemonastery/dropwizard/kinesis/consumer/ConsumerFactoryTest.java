package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.ConfigurationFactories;
import io.codemonastery.dropwizard.kinesis.FakeEvent;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationFactory;
import org.junit.Test;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @NotNull
        @Valid
        public ConsumerFactory<String> consumer;

    }

    @Test
    public void inferClassUsingAnonymousClass() throws Exception {
        assertThat(new ConsumerFactory<FakeEvent>(){}.inferEventClass())
        .isSameAs(FakeEvent.class);
    }

    @Test
    public void minimalConfig() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("consumer:\n  streamName: xyz"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.consumer.getStreamName()).isEqualTo("xyz");
    }

    @Test
    public void canSetSomeKclConfigs() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("consumer:\n  streamName: xyz\n  initialPositionInStream: TRIM_HORIZON"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.consumer.getStreamName()).isEqualTo("xyz");
        assertThat(configuration.consumer.getInitialPositionInStream()).isEqualTo(InitialPositionInStream.TRIM_HORIZON);
    }
}
