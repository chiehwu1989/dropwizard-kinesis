package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
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
        if(healthChecks != null){
            healthChecks.register(name, new ProducerHealthCheck(producerMetrics, kinesis, getStreamName()));
        }
        SimpleProducer<E> producer = new SimpleProducer<>(kinesis, getStreamName(), partitionKeyFn, encoder, producerMetrics);
        if(lifecycle != null){
            lifecycle.manage(producer);
        }
        return producer;
    }
}
