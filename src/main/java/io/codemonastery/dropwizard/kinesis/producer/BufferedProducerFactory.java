package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.util.Duration;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BufferedProducerFactory<E> extends AbstractProducerFactory<E> {

    @Min(1)
    private int deliveryThreadCount = 10;

    @Min(1)
    @Max(500)
    private int maxBufferSize = 100;

    @Valid
    @NotNull
    private Duration flushPeriod = Duration.seconds(10);

    @JsonProperty
    public int getDeliveryThreadCount() {
        return deliveryThreadCount;
    }

    @JsonProperty
    public void setDeliveryThreadCount(int deliveryThreadCount) {
        this.deliveryThreadCount = deliveryThreadCount;
    }

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

        ScheduledExecutorService deliveryExecutor;
        if(lifecycle != null){
            deliveryExecutor = lifecycle
                    .scheduledExecutorService(name + "-delivery-executor")
                    .threads(deliveryThreadCount).build();
        }else{
            deliveryExecutor = Executors.newScheduledThreadPool(deliveryThreadCount);
        }

        BufferedProducerMetrics producerMetrics = new BufferedProducerMetrics(metrics, name);
        if(healthChecks != null){
            healthChecks.register(name, new ProducerHealthCheck(producerMetrics, kinesis, getStreamName()));
        }
        BufferedProducer<E> producer = new BufferedProducer<>(kinesis, getStreamName(), partitionKeyFn, encoder, maxBufferSize, deliveryExecutor, producerMetrics);
        if(lifecycle != null){
            lifecycle.manage(producer);
        }

        deliveryExecutor.scheduleAtFixedRate(producer::flush,
                flushPeriod.toMilliseconds(),
                flushPeriod.toMilliseconds(),
                TimeUnit.MILLISECONDS);

        return producer;
    }

}
