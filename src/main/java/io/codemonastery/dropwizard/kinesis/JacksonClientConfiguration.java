package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.DnsResolver;
import com.amazonaws.retry.RetryPolicy;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.security.SecureRandom;

public class JacksonClientConfiguration extends ClientConfiguration {

    public JacksonClientConfiguration() {
    }

    public JacksonClientConfiguration(ClientConfiguration other) {
        super(other);
    }

    @JsonIgnore
    @Override
    public SecureRandom getSecureRandom() {
        return super.getSecureRandom();
    }

    @JsonIgnore
    @Override
    public void setSecureRandom(SecureRandom secureRandom) {
        super.setSecureRandom(secureRandom);
    }

    @JsonIgnore
    @Override
    public RetryPolicy getRetryPolicy() {
        return super.getRetryPolicy();
    }

    @JsonIgnore
    @Override
    public void setRetryPolicy(RetryPolicy retryPolicy) {
        super.setRetryPolicy(retryPolicy);
    }

    @JsonIgnore
    @Override
    public ClientConfiguration withRetryPolicy(RetryPolicy retryPolicy) {
        return super.withRetryPolicy(retryPolicy);
    }

    @JsonIgnore
    @Override
    public DnsResolver getDnsResolver() {
        return super.getDnsResolver();
    }

    @JsonIgnore
    @Override
    public void setDnsResolver(DnsResolver resolver) {
        super.setDnsResolver(resolver);
    }

    @JsonIgnore
    @Override
    public ClientConfiguration withDnsResolver(DnsResolver resolver) {
        return super.withDnsResolver(resolver);
    }
}
