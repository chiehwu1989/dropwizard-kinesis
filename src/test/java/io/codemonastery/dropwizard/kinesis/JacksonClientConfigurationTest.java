package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SystemDefaultDnsResolver;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.junit.Test;

import javax.validation.Valid;

import java.security.SecureRandom;

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

    @Test
    public void copyConstructDefaultNoExceptions() throws Exception {
         new JacksonClientConfiguration(new ClientConfiguration());
    }

    @Test
    public void maxRetryCanBeAnyValue() throws Exception {
        JacksonClientConfiguration config = new JacksonClientConfiguration();
        assertThat(config.withMaxErrorRetry(-1).getMaxErrorRetry()).isEqualTo(0);
        assertThat(config.withMaxErrorRetry(0).getMaxErrorRetry()).isEqualTo(0);
        assertThat(config.withMaxErrorRetry(1).getMaxErrorRetry()).isEqualTo(1);
    }

    @Test
    public void secureRandom() throws Exception {
        JacksonClientConfiguration config = new JacksonClientConfiguration();
        SecureRandom secureRandom = new SecureRandom();
        assertThat(config.withSecureRandom(secureRandom).getSecureRandom()).isSameAs(secureRandom);
    }

    @Test
    public void retryPolicy() throws Exception {
        JacksonClientConfiguration config = new JacksonClientConfiguration();
        RetryPolicy retryPolicy = new RetryPolicy(
                RetryPolicy.RetryCondition.NO_RETRY_CONDITION,
                RetryPolicy.BackoffStrategy.NO_DELAY,
                10,
                true);
        assertThat(config.withRetryPolicy(retryPolicy).getRetryPolicy()).isSameAs(retryPolicy);
    }

    @Test
    public void dnsResolver() throws Exception {
        JacksonClientConfiguration config = new JacksonClientConfiguration();
        SystemDefaultDnsResolver resolver = new SystemDefaultDnsResolver();
        assertThat(config.withDnsResolver(resolver).getDnsResolver()).isSameAs(resolver);
    }

    @Test
    public void socketBufferHints() throws Exception {
        JacksonClientConfiguration config = new JacksonClientConfiguration();
        assertThat(config.withSocketBufferSizeHints(100, 200).getSocketBufferSizeHints()).isEqualTo(new int[]{100, 200});
    }
}
