package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import io.codemonastery.dropwizard.kinesis.EventObjectMapper;
import io.codemonastery.dropwizard.kinesis.StreamConfiguration;
import io.codemonastery.dropwizard.kinesis.producer.ratelimit.AcquireLimiterFactory;
import io.codemonastery.dropwizard.kinesis.producer.ratelimit.DynamicAcquireLimiterFactory;
import io.dropwizard.setup.Environment;

import java.util.Objects;
import java.util.function.Function;

public abstract class AbstractProducerFactory<E> extends StreamConfiguration implements ProducerFactory<E> {

    protected Function<E, String> partitionKeyFn = Objects::toString;

    protected EventEncoder<E> encoder;

    protected AcquireLimiterFactory rateLimit = new DynamicAcquireLimiterFactory();

    @JsonIgnore
    @Override
    public ProducerFactory<E> streamName(String streamName) {
        super.setStreamName(streamName);
        return this;
    }

    @JsonIgnore
    @Override
    public Function<E, String> getPartitionKeyFn() {
        return partitionKeyFn;
    }

    @JsonIgnore
    @Override
    public void setPartitionKeyFn(Function<E, String> partitionKeyFn) {
        this.partitionKeyFn = partitionKeyFn;
    }

    @JsonIgnore
    @Override
    public ProducerFactory<E> partitionKeyFn(Function<E, String> partitionKeyFn) {
        this.setPartitionKeyFn(partitionKeyFn);
        return this;
    }

    @JsonIgnore
    @Override
    public EventEncoder<E> getEncoder() {
        return encoder;
    }

    @JsonIgnore
    @Override
    public void setEncoder(EventEncoder<E> encoder) {
        this.encoder = encoder;
    }

    @JsonIgnore
    @Override
    public ProducerFactory<E> encoder(EventEncoder<E> encoder) {
        this.setEncoder(encoder);
        return this;
    }

    @JsonProperty
    public AcquireLimiterFactory getRateLimit() {
        return rateLimit;
    }

    @JsonProperty
    public void setRateLimit(AcquireLimiterFactory rateLimit) {
        this.rateLimit = rateLimit;
    }

    @JsonIgnore
    @Override
    public ProducerFactory<E> rateLimit(AcquireLimiterFactory rateLimit){
        this.setRateLimit(rateLimit);
        return this;
    }

    @JsonIgnore
    @Override
    public Producer<E> build(Environment environment, AmazonKinesis kinesis, String name){
        if(encoder == null && environment != null){
            encoder = new EventObjectMapper<>(environment.getObjectMapper(), null);
        }
        return build(environment == null ? null : environment.metrics(),
                environment == null ? null : environment.healthChecks(),
                environment == null ? null : environment.lifecycle(),
                kinesis,
                name);
    }

}
