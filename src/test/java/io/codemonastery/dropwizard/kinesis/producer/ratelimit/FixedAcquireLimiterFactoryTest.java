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

public class FixedAcquireLimiterFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @NotNull
        @Valid
        public AcquireLimiterFactory rateLimit;

    }

    @Test
    public void typeFixed() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("rateLimit:\n  type: fixed\n"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.rateLimit).isInstanceOf(FixedAcquireLimiterFactory.class);
    }

    @Test
    public void canConfigure() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("rateLimit:\n  type: fixed\n  perSecond: 500"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.rateLimit).isInstanceOf(FixedAcquireLimiterFactory.class);
        assertThat(((FixedAcquireLimiterFactory) configuration.rateLimit).getPerSecond()).isEqualTo(500.0);
    }

    @Test
    public void allTheThings() throws Exception {
        FixedAcquireLimiterFactory factory = new FixedAcquireLimiterFactory().perSecond(500);
        assertThat(factory.getPerSecond()).isEqualTo(500.0);
        assertThat(factory.build()).isNotNull();
    }
}
