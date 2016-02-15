package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class KinesisProducerFactory<E> extends KinesisStreamConfiguration {

    @Min(1)
    private int maxBufferSize = 1000;

    @Valid
    @NotNull
    private Duration flushPeriod = Duration.seconds(10);

    private EventEncoder<E> encoder;

    private Function<E, String> partitionKeyFn = Object::toString;

    @JsonProperty
    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    @JsonProperty
    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    @JsonProperty
    public Duration getFlushPeriod() {
        return flushPeriod;
    }

    @JsonProperty
    public void setFlushPeriod(Duration flushPeriod) {
        this.flushPeriod = flushPeriod;
    }

    @JsonIgnore
    public KinesisProducerFactory encoder(EventEncoder<E> encoder) {
        this.encoder = encoder;
        return this;
    }

    @JsonIgnore
    public KinesisProducerFactory partitionKeyFn(Function<E, String> partitionKeyFn) {
        this.partitionKeyFn = partitionKeyFn;
        return this;
    }

    @JsonIgnore
    public KinesisProducer<E> build(Environment environment, AmazonKinesis client, String name){
        if(encoder == null && environment != null){
            encoder = new EventObjectMapper<>(environment.getObjectMapper());
        }
        return build(environment == null ? null : environment.metrics(),
                environment == null ? null : environment.healthChecks(),
                environment == null ? null : environment.lifecycle(),
                client, name);
    }

    @JsonIgnore
    private KinesisProducer<E> build(MetricRegistry metrics,
                                     HealthCheckRegistry healthChecks,
                                     LifecycleEnvironment lifecycle,
                                     AmazonKinesis client,
                                     String name) {
        Preconditions.checkNotNull(encoder, "encoder cannot be null, was not inferred");
        Preconditions.checkNotNull(partitionKeyFn, "partitionKeyFn cannot be null, is allowed to return null");
        Preconditions.checkNotNull(flushPeriod, "flushPeriod cannot be null");
        Preconditions.checkArgument(flushPeriod.getQuantity() > 0, "flush period must be positive");

        super.setupStream(client);

        ScheduledExecutorService deliveryExecutor = lifecycle
                .scheduledExecutorService(name + "-delivery-executor")
                .threads(2).build();

        KinesisProducer<E> producer = new KinesisProducer<>(client, getStreamName(), maxBufferSize, deliveryExecutor, encoder, partitionKeyFn);

        deliveryExecutor.scheduleAtFixedRate(producer::flush,
                flushPeriod.toMilliseconds(),
                flushPeriod.toMilliseconds(),
                TimeUnit.MILLISECONDS);

        return producer;
    }

}
