package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import io.codemonastery.dropwizard.kinesis.StreamCreateConfiguration;
import io.codemonastery.dropwizard.kinesis.healthcheck.StreamHealthCheck;
import io.codemonastery.dropwizard.kinesis.producer.ratelimit.AcquireLimiterFactory;
import io.codemonastery.dropwizard.kinesis.producer.ratelimit.NoLimitAcquireLimiter;
import io.codemonastery.dropwizard.kinesis.producer.ratelimit.RateLimitedRecordPutter;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;

import java.util.function.Function;

public class SimpleProducerFactory<E> extends AbstractProducerFactory<E> {

    @Override
    public SimpleProducerFactory<E> streamName(String streamName) {
        super.streamName(streamName);
        return this;
    }

    @Override
    public SimpleProducerFactory<E> partitionKeyFn(Function<E, String> partitionKeyFn) {
        super.partitionKeyFn(partitionKeyFn);
        return this;
    }

    @Override
    public SimpleProducerFactory<E> encoder(EventEncoder<E> encoder) {
        super.encoder(encoder);
        return this;
    }

    @JsonIgnore
    public SimpleProducerFactory<E> create(StreamCreateConfiguration create) {
        this.setCreate(create);
        return this;
    }

    @Override
    public SimpleProducerFactory<E> rateLimit(AcquireLimiterFactory rateLimit) {
        super.rateLimit(rateLimit);
        return this;
    }

    @Override
    public SimpleProducer<E> build(MetricRegistry metrics,
                                   HealthCheckRegistry healthChecks,
                                   LifecycleEnvironment lifecycle,
                                   AmazonKinesis kinesis,
                                   String name) {
        Preconditions.checkNotNull(encoder, "encoder cannot be null, was not inferred");
        Preconditions.checkNotNull(partitionKeyFn, "partitionKeyFn cannot be null, is allowed to return null");
        Preconditions.checkState(super.setupStream(kinesis), String.format("stream %s was not setup successfully", getStreamName()));

        ProducerMetrics producerMetrics = new ProducerMetrics(metrics, name);
        if (healthChecks != null) {
            healthChecks.register(name, new StreamFailureCheck(producerMetrics, new StreamHealthCheck(kinesis, getStreamName())));
        }
        SimpleProducer<E> producer = new SimpleProducer<>(
                getStreamName(),
                partitionKeyFn,
                encoder,
                producerMetrics,
                new RateLimitedRecordPutter(
                        kinesis,
                        producerMetrics,
                        Optional.fromNullable(rateLimit).or(NoLimitAcquireLimiter::new).build()
                )
        );
        if (lifecycle != null) {
            lifecycle.manage(producer);
        }
        return producer;
    }
}
