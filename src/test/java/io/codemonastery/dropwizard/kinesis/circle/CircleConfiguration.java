package io.codemonastery.dropwizard.kinesis.circle;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.DynamoDbFactory;
import io.codemonastery.dropwizard.kinesis.KinesisFactory;
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
    private KinesisFactory kinesis = new KinesisFactory();

    @Valid
    @NotNull
    private DynamoDbFactory dynamoDb = new DynamoDbFactory();

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
    public KinesisConsumerFactory<String> getConsumer() {
        return consumer;
    }

    @JsonProperty
    public void setConsumer(KinesisConsumerFactory<String> consumer) {
        this.consumer = consumer;
    }
}
