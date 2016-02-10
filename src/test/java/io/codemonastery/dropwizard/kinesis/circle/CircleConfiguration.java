package io.codemonastery.dropwizard.kinesis.circle;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.ConsumerProcessFactory;
import io.codemonastery.dropwizard.kinesis.SendServiceFactory;
import io.dropwizard.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class CircleConfiguration extends Configuration {
    //
//    Name
//
    @Valid
    @NotNull
    private String name;

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    //
//    SEND SERVICE
//
    @Valid
    @NotNull
    private SendServiceFactory sendService;

    @JsonProperty("sendService")
    public SendServiceFactory getSendService() {
        return sendService;
    }


    @JsonProperty("sendService")
    public void setSendService(SendServiceFactory sendService) {
        this.sendService = sendService;
    }


    //
//    STREAM PROCESS
//
    @Valid
    @NotNull
    @JsonProperty
    private ConsumerProcessFactory consumerProcessFactory = new ConsumerProcessFactory();

    @JsonProperty("processFactory")
    public ConsumerProcessFactory getConsumerProcessFactory() {
        return consumerProcessFactory;
    }

    @JsonProperty("processFactory")
    public void setConsumerProcessFactory(ConsumerProcessFactory consumerProcessFactory) {
        this.consumerProcessFactory = consumerProcessFactory;
    }
}
