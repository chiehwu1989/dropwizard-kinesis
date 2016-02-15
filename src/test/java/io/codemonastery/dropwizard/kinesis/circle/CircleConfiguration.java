package io.codemonastery.dropwizard.kinesis.circle;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.DynamoDbClientBuilder;
import io.codemonastery.dropwizard.kinesis.KinesisClientBuilder;
import io.codemonastery.dropwizard.kinesis.KinesisConsumerFactory;
import io.codemonastery.dropwizard.kinesis.KinesisProducerFactory;
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
    private KinesisProducerFactory<String> producer = new KinesisProducerFactory<>();

    @Valid
    @NotNull
    private KinesisConsumerFactory<String> consumer = new KinesisConsumerFactory<>();

    public CircleConfiguration() {
        kinesis.setRegion(KinesisClientRule.TEST_REGIONS);
        dynamoDb.setRegion(KinesisClientRule.TEST_REGIONS);
        producer.setStreamName("test-circle");
        producer.setDefaultShardCount(1);
        producer.setFlushPeriod(Duration.seconds(1));

        consumer.setStreamName("test-circle");
        consumer.setDefaultShardCount(1);
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
    public KinesisProducerFactory<String> getProducer() {
        return producer;
    }

    @JsonProperty
    public void setProducer(KinesisProducerFactory<String> producer) {
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
