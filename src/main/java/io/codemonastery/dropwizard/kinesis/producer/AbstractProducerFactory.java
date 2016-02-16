package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import io.codemonastery.dropwizard.kinesis.EventObjectMapper;
import io.codemonastery.dropwizard.kinesis.StreamConfiguration;
import io.dropwizard.setup.Environment;

import java.util.Objects;
import java.util.function.Function;

public abstract class AbstractProducerFactory<E> extends StreamConfiguration implements ProducerFactory<E> {

    protected EventEncoder<E> encoder;

    protected Function<E, String> partitionKeyFn = Objects::toString;

    @Override
    public ProducerFactory<E> streamName(String streamName) {
        super.setStreamName(streamName);
        return this;
    }

    @JsonIgnore
    @Override
    public ProducerFactory<E> encoder(EventEncoder<E> encoder) {
        this.encoder = encoder;
        return this;
    }

    @JsonIgnore
    @Override
    public ProducerFactory<E> partitionKeyFn(Function<E, String> partitionKeyFn) {
        this.partitionKeyFn = partitionKeyFn;
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
