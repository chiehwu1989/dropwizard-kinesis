package io.codemonastery.dropwizard.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.consumer.ConsumerFactory;
import io.codemonastery.dropwizard.kinesis.producer.ProducerFactory;
import io.dropwizard.Configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class ReadmeConfiguration extends Configuration {

    @Valid
    @NotNull
    private KinesisFactory kinesis = new KinesisFactory();

    @Valid
    @NotNull
    private DynamoDbFactory dynamoDb = new DynamoDbFactory();

    //no need to initialize since we can choose between simple and buffered produces via configuration
    @Valid
    @NotNull
    private ProducerFactory<Event> producer;

    //the anonymous subclass here allows us to infer json parsing for the Event class
    @Valid
    @NotNull
    private ConsumerFactory<Event> consumer = new ConsumerFactory<Event>(){};

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
    public ProducerFactory<Event> getProducer() {
        return producer;
    }

    @JsonProperty
    public void setProducer(ProducerFactory<Event> producer) {
        this.producer = producer;
    }

    @JsonProperty
    public ConsumerFactory<Event> getConsumer() {
        return consumer;
    }

    @JsonProperty
    public void setConsumer(ConsumerFactory<Event> consumer) {
        this.consumer = consumer;
    }
}
