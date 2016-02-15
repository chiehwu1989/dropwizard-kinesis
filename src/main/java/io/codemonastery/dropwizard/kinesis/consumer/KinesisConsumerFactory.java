package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleWorker;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.codemonastery.dropwizard.kinesis.*;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public class KinesisConsumerFactory<E> extends StreamConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisConsumerFactory.class);

    private EventDecoder<E> decoder;

    private Supplier<EventConsumer<E>> processorFactory;

    @JsonIgnore
    public KinesisConsumerFactory decoder(EventDecoder<E> decoder) {
        this.decoder = decoder;
        return this;
    }

    @JsonIgnore
    public KinesisConsumerFactory processor(Supplier<EventConsumer<E>> processorFactory) {
        this.processorFactory = processorFactory;
        return this;
    }

    @JsonIgnore
    public SimpleWorker build(Environment environment,
                              AmazonKinesis kinesisClient,
                              AmazonDynamoDB dynamoDBClient,
                              String name) {
        if (environment != null && decoder == null) {
            decoder = new EventObjectMapper<>(environment.getObjectMapper());
        }
        if (processorFactory == null) {
            processorFactory = () -> event -> {
                if (event != null) {
                    LOG.info("Consumed event on " + name + ": " + event.toString());
                }
                return true;
            };
        }

        return build(
                environment == null ? null : environment.metrics(),
                environment == null ? null : environment.healthChecks(),
                environment == null ? null : environment.lifecycle(),
                kinesisClient,
                dynamoDBClient,
                name
        );
    }

    @JsonIgnore
    public SimpleWorker build(MetricRegistry metrics,
                              HealthCheckRegistry healthChecks,
                              LifecycleEnvironment lifeCycle,
                              AmazonKinesis kinesisClient,
                              AmazonDynamoDB dynamoDBClient,
                              String name) {
        super.setupStream(kinesisClient);

        final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
                name,
                getStreamName(),
                null,
                name
        );


        SimpleWorker.Builder builder = new SimpleWorker.Builder()
                .recordProcessorFactory(() -> new RecordProcessor<>(decoder, processorFactory.get()))
                .config(config)
                .kinesisClient(kinesisClient)
                .dynamoDBClient(dynamoDBClient);

        //use unbounded queue
        if (lifeCycle != null) {
            ExecutorService processorService = lifeCycle.executorService(name + "-processor-%d")
                    .build();
            builder.execService(processorService).build();
        }

        SimpleWorker worker = builder.build();

        if (lifeCycle == null) {
            new Thread(worker::run) {
                {
                    setDaemon(true);
                }
            }.start();
        } else {
            lifeCycle.executorService(name + "-consumer-worker")
                    .minThreads(1).maxThreads(1)
                    .build().submit(worker::run);
        }

        return worker;
    }
}
