package io.codemonastery.dropwizard.kinesis;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;

public class ConfigurationFactories {

    public static <C extends Configuration> ConfigurationFactory<C> make(Class<C> klass){
        ObjectMapper objectMapper = Jackson.newObjectMapper();
        objectMapper.copy().enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return new ConfigurationFactory<>(klass, Validators.newValidator(), objectMapper, "dw");
    }

    private ConfigurationFactories() {
    }
}
