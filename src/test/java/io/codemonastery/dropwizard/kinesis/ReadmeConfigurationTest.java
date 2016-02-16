package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.util.StringInputStream;
import io.dropwizard.configuration.ConfigurationFactory;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class ReadmeConfigurationTest {

    @Test
    public void parseMinimal() throws Exception {
        ConfigurationFactory<ReadmeConfiguration> configurationFactory = ConfigurationFactories.make(ReadmeConfiguration.class);
        ReadmeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("consumer:\n  streamName: test-stream\n\nproducer:\n  streamName: test-stream"), "");
        assertThat(configuration).isNotNull();
    }
}
