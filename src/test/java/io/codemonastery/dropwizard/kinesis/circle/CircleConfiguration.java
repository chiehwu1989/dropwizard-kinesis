package io.codemonastery.dropwizard.kinesis.circle;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.DynamoDbFactory;
import io.codemonastery.dropwizard.kinesis.KinesisFactory;
import io.codemonastery.dropwizard.kinesis.consumer.ConsumerFactory;
import io.codemonastery.dropwizard.kinesis.producer.BufferedProducerFactory;
import io.dropwizard.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class CircleConfiguration extends Configuration {

    @Valid
    @NotNull
    private KinesisFactory kinesis = new KinesisFactory();

    @Valid
    @NotNull
    private DynamoDbFactory dynamoDb = new DynamoDbFactory();

    @Valid
    @NotNull
    private BufferedProducerFactory<String> producer = new BufferedProducerFactory<>();

    @Valid
    @NotNull
    private ConsumerFactory<String> consumer = new ConsumerFactory<String>(){};

    @JsonProperty
    public KinesisFactory getKinesis() {
        return kinesis;
    }

    @JsonProperty
    public void setKinesis(KinesisFactory kinesis) {
        this.kinesis = kinesis;
    }

    @JsonProperty
    public DynamoDbFactory getDynamoDb() {
        return dynamoDb;
    }

    @JsonProperty
    public void setDynamoDb(DynamoDbFactory dynamoDb) {
        this.dynamoDb = dynamoDb;
    }

    @JsonProperty
    public BufferedProducerFactory<String> getProducer() {
        return producer;
    }

    @JsonProperty
    public void setProducer(BufferedProducerFactory<String> producer) {
        this.producer = producer;
    }

    @JsonProperty
    public ConsumerFactory<String> getConsumer() {
        return consumer;
    }

    @JsonProperty
    public void setConsumer(ConsumerFactory<String> consumer) {
        if(consumer != null){
            consumer.inheritDecoder(this.consumer);
        }
        this.consumer = consumer;
    }
}
