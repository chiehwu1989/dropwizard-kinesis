package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleWorker;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.codemonastery.dropwizard.kinesis.EventDecoder;
import io.codemonastery.dropwizard.kinesis.EventObjectMapper;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public class ConsumerFactory<E> extends KinesisClientLibConfig {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerFactory.class);

    private EventDecoder<E> decoder;

    private Supplier<EventConsumer<E>> eventConsumerFactory;



    @JsonIgnore
    public ConsumerFactory decoder(EventDecoder<E> decoder) {
        this.decoder = decoder;
        return this;
    }

    @JsonIgnore
    public ConsumerFactory consumer(Supplier<EventConsumer<E>> eventConsumerFactory) {
        this.eventConsumerFactory = eventConsumerFactory;
        return this;
    }



    @JsonIgnore
    public SimpleWorker build(Environment environment,
                              AmazonKinesis kinesis,
                              AmazonDynamoDB dynamoDb,
                              String name) {
        if (environment != null && decoder == null) {
            decoder = new EventObjectMapper<>(environment.getObjectMapper());
        }
        if (eventConsumerFactory == null) {
            eventConsumerFactory = () -> event -> {
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
                kinesis,
                dynamoDb,
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

        SimpleWorker.Builder builder = new SimpleWorker.Builder()
                .recordProcessorFactory(() -> new RecordProcessor<>(decoder, eventConsumerFactory.get()))
                .config(makeKinesisClientLibConfiguration(name))
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
