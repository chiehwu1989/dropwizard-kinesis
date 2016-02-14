package io.codemonastery.dropwizard.kinesis.circle;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.KinesisClientBuilder;
import io.codemonastery.dropwizard.kinesis.KinesisProducerFactory;
import io.dropwizard.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class CircleConfiguration extends Configuration {

    @Valid
    @NotNull
    private KinesisClientBuilder kinesis = new KinesisClientBuilder();

    @Valid
    @NotNull
    private KinesisProducerFactory<String> producer = new KinesisProducerFactory<>();

    @JsonProperty
    public KinesisClientBuilder getKinesis() {
        return kinesis;
    }

    @JsonProperty
    public void setKinesis(KinesisClientBuilder kinesis) {
        this.kinesis = kinesis;
    }

    @JsonProperty
    public KinesisProducerFactory<String> getProducer() {
        return producer;
    }

    @JsonProperty
    public void setProducer(KinesisProducerFactory<String> producer) {
        this.producer = producer;
    }
}
