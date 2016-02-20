package io.codemonastery.dropwizard.kinesis.example;

import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.codemonastery.dropwizard.kinesis.ConfigurationFactories;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class ExampleConfigurationTest {

    @Test
    public void parseMinimal() throws Exception {
        ConfigurationFactory<ExampleConfiguration> configurationFactory = ConfigurationFactories.make(ExampleConfiguration.class);
        ExampleConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("kinesis:\n  region: US_WEST_2\ndynamoDb:\n  region: US_WEST_2\nconsumer:\n  streamName: test-stream\n\nproducer:\n  streamName: test-stream"), "");
        assertThat(configuration).isNotNull();
    }

    @Test
    public void emitComplete() throws Exception {
        ConfigurationFactory<ExampleConfiguration> configurationFactory = ConfigurationFactories.make(ExampleConfiguration.class);
        ExampleConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("kinesis:\n  region: US_WEST_2\ndynamoDb:\n  region: US_WEST_2\nconsumer:\n  streamName: test-stream\n\nproducer:\n  streamName: test-stream"), "");
        ObjectMapper objectMapper = Jackson.newObjectMapper();
        YAMLFactory yamlFactory = new YAMLFactory();
        objectMapper.writeValue(yamlFactory.createGenerator(System.out), configuration);
    }
}
