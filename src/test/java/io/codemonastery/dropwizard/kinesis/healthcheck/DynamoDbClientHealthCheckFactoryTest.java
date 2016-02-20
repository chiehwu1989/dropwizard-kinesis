package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.ConfigurationFactories;
import io.dropwizard.Configuration;
import org.junit.Test;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import static org.assertj.core.api.Assertions.assertThat;


public class DynamoDbClientHealthCheckFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @NotNull
        @Valid
        public DynamoDbClientHealthCheckFactory healthCheck;

    }

    @Test
    public void defaults() throws Exception {
        FakeConfiguration configuration = ConfigurationFactories.make(FakeConfiguration.class).build(s -> new StringInputStream("healthCheck: {}"), "");
        assertThat(configuration.healthCheck).isNotNull();
        assertThat(configuration.healthCheck).isInstanceOf(DescribeTableHealthCheckFactory.class);
    }
}
