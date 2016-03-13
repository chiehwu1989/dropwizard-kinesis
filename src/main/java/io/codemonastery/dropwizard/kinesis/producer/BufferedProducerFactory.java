package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import io.codemonastery.dropwizard.kinesis.StreamCreateConfiguration;
import io.codemonastery.dropwizard.kinesis.healthcheck.StreamHealthCheck;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.util.Duration;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.*;
import java.util.function.Function;

public class BufferedProducerFactory<E> extends AbstractProducerFactory<E> {

    @Min(1)
    @Max(500)
    private int maxBufferSize = 100;

    @Valid
    @NotNull
    private Duration flushPeriod = Duration.seconds(10);

    @JsonProperty
    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    @JsonProperty
    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    @JsonIgnore
    public BufferedProducerFactory<E> maxBufferSize(int maxBufferSize) {
        this.setMaxBufferSize(maxBufferSize);
        return this;
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
    public BufferedProducerFactory<E> flushPeriod(Duration flushPeriod) {
        this.setFlushPeriod(flushPeriod);
        return this;
    }

    @Override
    public BufferedProducerFactory<E> streamName(String streamName) {
        super.streamName(streamName);
        return this;
    }

    @Override
    public BufferedProducerFactory<E> partitionKeyFn(Function<E, String> partitionKeyFn) {
        super.partitionKeyFn(partitionKeyFn);
        return this;
    }

    @Override
    public BufferedProducerFactory<E> encoder(EventEncoder<E> encoder) {
        super.encoder(encoder);
        return this;
    }

    @JsonIgnore
    public BufferedProducerFactory<E> create(StreamCreateConfiguration create){
        this.setCreate(create);
        return this;
    }

    @JsonIgnore
    public BufferedProducer<E> build(MetricRegistry metrics,
                                     HealthCheckRegistry healthChecks,
                                     LifecycleEnvironment lifecycle,
                                     AmazonKinesis kinesis,
                                     String name) {
        Preconditions.checkNotNull(encoder, "encoder cannot be null, was not inferred");
        Preconditions.checkNotNull(partitionKeyFn, "partitionKeyFn cannot be null, is allowed to return null");
        Preconditions.checkNotNull(flushPeriod, "flushPeriod cannot be null");
        Preconditions.checkArgument(flushPeriod.getQuantity() > 0, "flush period must be positive");
        Preconditions.checkState(super.setupStream(kinesis), String.format("stream %s was not setup successfully", getStreamName()));

        final ExecutorService deliveryExecutor;
        final ScheduledExecutorService flushExecutor;
        if(lifecycle != null){
            deliveryExecutor = lifecycle.executorService(name + "-delivery-executor-%d")
                    .workQueue(new SingletonBlockOnSubmitQueue())
                    .minThreads(1)
                    .maxThreads(1)
                    .build();
            flushExecutor = lifecycle
                    .scheduledExecutorService(name + "-flush-executor-%d")
                    .threads(1)
                    .build();
        }else{
            deliveryExecutor = Executors.newSingleThreadExecutor();
            flushExecutor = Executors.newScheduledThreadPool(1);
        }

        BufferedProducerMetrics producerMetrics = new BufferedProducerMetrics(metrics, name);
        if(healthChecks != null){
            healthChecks.register(name, new StreamFailureCheck(producerMetrics, new StreamHealthCheck(kinesis, getStreamName())));
        }
        BufferedProducer<E> producer = new BufferedProducer<>(kinesis, getStreamName(), partitionKeyFn, encoder, maxBufferSize, deliveryExecutor, producerMetrics);
        if(lifecycle != null){
            lifecycle.manage(producer);
        }

        flushExecutor.scheduleAtFixedRate(producer::flush,
                flushPeriod.toMilliseconds(),
                flushPeriod.toMilliseconds(),
                TimeUnit.MILLISECONDS);

        return producer;
    }
}
