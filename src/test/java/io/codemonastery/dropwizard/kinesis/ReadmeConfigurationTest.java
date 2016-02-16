package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class ReadmeConfigurationTest {

    @Test
    public void parseMinimal() throws Exception {
        ConfigurationFactory<ReadmeConfiguration> configurationFactory = ConfigurationFactories.make(ReadmeConfiguration.class);
        ReadmeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("consumer:\n  streamName: test-stream\n\nproducer:\n  streamName: test-stream"), "");
        assertThat(configuration).isNotNull();
    }

    @Test
    public void emitComplete() throws Exception {
        ConfigurationFactory<ReadmeConfiguration> configurationFactory = ConfigurationFactories.make(ReadmeConfiguration.class);
        ReadmeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("consumer:\n  streamName: test-stream\n\nproducer:\n  streamName: test-stream"), "");
        ObjectMapper objectMapper = Jackson.newObjectMapper();
        YAMLFactory yamlFactory = new YAMLFactory();
        objectMapper.writeValue(yamlFactory.createGenerator(System.out), configuration);
    }
}
