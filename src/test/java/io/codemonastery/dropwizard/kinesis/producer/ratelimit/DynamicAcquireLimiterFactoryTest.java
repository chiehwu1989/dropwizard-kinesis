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

public class DynamicAcquireLimiterFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @NotNull
        @Valid
        public AcquireLimiterFactory rateLimit;

    }

    @Test
    public void usesDynamicByDefault() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("rateLimit: {}\n"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.rateLimit).isInstanceOf(DynamicAcquireLimiterFactory.class);
    }

    @Test
    public void typeDynamic() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("rateLimit:\n  type: dynamic\n"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.rateLimit).isInstanceOf(DynamicAcquireLimiterFactory.class);
    }

    @Test
    public void canConfigure() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("rateLimit:\n  type: dynamic\n  initialPerSecond: 500"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.rateLimit).isInstanceOf(DynamicAcquireLimiterFactory.class);
        assertThat(((DynamicAcquireLimiterFactory) configuration.rateLimit).getInitialPerSecond()).isEqualTo(500.0);
    }

    @Test
    public void allTheThings() throws Exception {
        DynamicAcquireLimiterFactory factory = new DynamicAcquireLimiterFactory().initialPerSecond(500);
        assertThat(factory.getInitialPerSecond()).isEqualTo(500.0);
        assertThat(factory.build()).isNotNull();
    }
}
