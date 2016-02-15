package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;

import java.util.function.Function;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type",
        defaultImpl = SimpleProducerFactory.class
)
@JsonSubTypes({ @JsonSubTypes.Type(value = SimpleProducerFactory.class, name = "simple"), @JsonSubTypes.Type(value = BufferedProducerFactory.class, name = "buffered") })
public interface ProducerFactory<E> {

        String getStreamName();

        ProducerFactory<E> streamName(String streamName);

        ProducerFactory<E> encoder(EventEncoder<E> encoder);

        ProducerFactory<E> partitionKeyFn(Function<E, String> partitionKeyFn);

        Producer<E> build(Environment environment, AmazonKinesis client, String name);

        Producer<E> build(MetricRegistry metrics,
                                         HealthCheckRegistry healthChecks,
                                         LifecycleEnvironment lifecycle,
                                         AmazonKinesis client,
                                         String name);

}
