package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.ConfigurationValidationException;
import org.junit.Test;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class StreamConfigurationTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @NotNull
        @Valid
        public StreamConfiguration stream;

    }

    @Test
    public void exceptsIfMissingStream() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        try{
            configurationFactory.build((s) -> new StringInputStream("server:\n  type: simple"), "");
            fail("supposed to fail validation");
        }catch (ConfigurationValidationException e){
            assertThat(e.getMessage()).contains("stream may not be null");
        }
    }

    @Test
    public void canConfigureStreamName() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("stream:\n  streamName: xyz"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.stream).isNotNull();
        assertThat(configuration.stream.getStreamName()).isEqualTo("xyz");
        assertThat(configuration.stream.getCreate()).isNull();
    }

    @Test
    public void canConfigureStreamWithDefaultCreate() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("stream:\n  streamName: xyz\n  create: {}"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.stream).isNotNull();
        assertThat(configuration.stream.getStreamName()).isEqualTo("xyz");
        assertThat(configuration.stream.getCreate()).isNotNull();
    }

    @Test
    public void callSetupOnCreate() throws Exception {
        StreamCreateConfiguration create = mock(StreamCreateConfiguration.class);
        when(create.setupStream(any(AmazonKinesis.class), any(String.class))).thenReturn(false);

        StreamConfiguration configuration = new StreamConfiguration();
        configuration.setStreamName("xyz");
        configuration.setCreate(create);
        boolean result = configuration.setupStream(null);

        verify(create, times(1)).setupStream(any(AmazonKinesis.class), any(String.class));
        assertThat(result).isFalse();
    }
}
