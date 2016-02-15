package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.junit.Test;

import javax.validation.Valid;

import static org.assertj.core.api.Assertions.assertThat;

public class JacksonClientConfigurationTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @Valid
        public JacksonClientConfiguration client;

    }

    @Test
    public void emptyConfigurationIsOk() throws Exception {
        FakeConfiguration configuration = ConfigurationFactories.make(FakeConfiguration.class)
                .build((s)->new StringInputStream("client: {}"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.client).isNotNull();
    }

    @Test
    public void canActuallyConfigureStuff() throws Exception {
        FakeConfiguration configuration = ConfigurationFactories.make(FakeConfiguration.class)
                .build((s)->new StringInputStream("client:\n  connectionTimeout: 5000"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.client).isNotNull();
        assertThat(configuration.client.getConnectionTimeout()).isEqualTo(5000);
    }

}
