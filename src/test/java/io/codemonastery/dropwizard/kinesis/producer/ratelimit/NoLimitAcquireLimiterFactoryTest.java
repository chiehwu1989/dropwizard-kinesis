package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.ConfigurationFactories;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationFactory;
import org.junit.Test;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import static org.assertj.core.api.Assertions.assertThat;

public class NoLimitAcquireLimiterFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @NotNull
        @Valid
        public AcquireLimiterFactory rateLimit;

    }

    @Test
    public void noLimitType() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("rateLimit:\n  type: nolimit"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.rateLimit).isInstanceOf(NoLimitAcquireLimiterFactory.class);
    }

    @Test
    public void allTheThings() throws Exception {
        NoLimitAcquireLimiterFactory factory = new NoLimitAcquireLimiterFactory();
        assertThat(factory.build()).isNotNull();
    }

}
