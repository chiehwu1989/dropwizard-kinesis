package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;

public class SimpleProducerFactory<E> extends AbstractProducerFactory<E> {

    @Override
    public SimpleProducer<E> build(MetricRegistry metrics,
                                   HealthCheckRegistry healthChecks,
                                   LifecycleEnvironment lifecycle,
                                   AmazonKinesis kinesis,
                                   String name) {
        Preconditions.checkNotNull(encoder, "encoder cannot be null, was not inferred");
        Preconditions.checkNotNull(partitionKeyFn, "partitionKeyFn cannot be null, is allowed to return null");
        Preconditions.checkState(super.setupStream(kinesis), String.format("stream %s was not setup successfully", getStreamName()));
        return new SimpleProducer<>(kinesis, getStreamName(), partitionKeyFn, encoder);
    }
}