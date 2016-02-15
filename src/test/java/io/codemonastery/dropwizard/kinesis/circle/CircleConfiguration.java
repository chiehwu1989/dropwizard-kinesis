package io.codemonastery.dropwizard.kinesis.circle;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.DynamoDbClientBuilder;
import io.codemonastery.dropwizard.kinesis.KinesisClientBuilder;
import io.codemonastery.dropwizard.kinesis.consumer.KinesisConsumerFactory;
import io.codemonastery.dropwizard.kinesis.producer.BufferedProducerFactory;
import io.codemonastery.dropwizard.kinesis.rule.KinesisClientRule;
import io.dropwizard.Configuration;
import io.dropwizard.util.Duration;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class CircleConfiguration extends Configuration {

    @Valid
    @NotNull
    private KinesisClientBuilder kinesis = new KinesisClientBuilder();

    @Valid
    @NotNull
    private DynamoDbClientBuilder dynamoDb = new DynamoDbClientBuilder();

    @Valid
    @NotNull
    private BufferedProducerFactory<String> producer = new BufferedProducerFactory<>();

    @Valid
    @NotNull
    private KinesisConsumerFactory<String> consumer = new KinesisConsumerFactory<>();

    public CircleConfiguration() {
        kinesis.setRegion(KinesisClientRule.TEST_REGIONS);
        dynamoDb.setRegion(KinesisClientRule.TEST_REGIONS);
        producer.setStreamName("test-circle");
        producer.setFlushPeriod(Duration.seconds(1));

        consumer.setStreamName("test-circle");
    }

    @JsonProperty
    public KinesisClientBuilder getKinesis() {
        return kinesis;
    }

    @JsonProperty
    public void setKinesis(KinesisClientBuilder kinesis) {
        this.kinesis = kinesis;
    }

    @JsonProperty
    public DynamoDbClientBuilder getDynamoDb() {
        return dynamoDb;
    }

    @JsonProperty
    public void setDynamoDb(DynamoDbClientBuilder dynamoDb) {
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
    public KinesisConsumerFactory<String> getConsumer() {
        return consumer;
    }

    @JsonProperty
    public void setConsumer(KinesisConsumerFactory<String> consumer) {
        this.consumer = consumer;
    }
}
