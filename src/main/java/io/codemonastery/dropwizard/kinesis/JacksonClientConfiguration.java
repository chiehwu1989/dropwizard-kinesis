package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.ClientConfiguration;
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
}
