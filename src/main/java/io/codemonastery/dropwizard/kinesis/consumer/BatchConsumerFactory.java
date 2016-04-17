package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleWorker;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventDecoder;
import io.codemonastery.dropwizard.kinesis.EventObjectMapper;
import io.codemonastery.dropwizard.kinesis.StreamCreateConfiguration;
import io.codemonastery.dropwizard.kinesis.healthcheck.StreamHealthCheck;
import io.codemonastery.dropwizard.kinesis.producer.StreamFailureCheck;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.ParameterizedType;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public class BatchConsumerFactory<E> extends KinesisClientLibConfig {

    private static final Logger LOG = LoggerFactory.getLogger(BatchConsumerFactory.class);

    private BatchConsumerFactory<E> decoderInheritParent = null;

    private EventDecoder<E> decoder;
    private Supplier<BatchConsumer<E>> consumer;

    @JsonIgnore
    public BatchConsumerFactory<E> streamName(String streamName) {
        this.setStreamName(streamName);
        return this;
    }

    @JsonIgnore
    public EventDecoder<E> getDecoder() {
        return decoder;
    }

    @JsonIgnore
    public void setDecoder(EventDecoder<E> decoder) {
        this.decoder = decoder;
    }

    @JsonIgnore
    public BatchConsumerFactory<E> decoder(EventDecoder<E> decoder) {
        this.setDecoder(decoder);
        return this;
    }

    @JsonIgnore
    public Supplier<BatchConsumer<E>> getConsumer() {
        return consumer;
    }

    @JsonIgnore
    public void setConsumer(Supplier<BatchConsumer<E>> consumer) {
        this.consumer = consumer;
    }

    @JsonIgnore
    public BatchConsumerFactory<E> consumer(Supplier<BatchConsumer<E>> consumer) {
        this.setConsumer(consumer);
        return this;
    }

    @JsonIgnore
    public BatchConsumerFactory<E> create(StreamCreateConfiguration create){
        this.setCreate(create);
        return this;
    }

    @JsonIgnore
    public BatchConsumerFactory<E> applicationName(String applicationName){
        this.setApplicationName(applicationName);
        return this;
    }

    /**
     * Can form an inheritance chain of consumer factories such that this consumer factory will
     * use the paramterized factory to infer event decoder.
     *
     * Intended to be used in configuration.setConsumerFactory so that any parsed configured factories
     * will inherit from the one specified in your configuration class
     * @param other the factory
     * @return this factory
     */
    @JsonIgnore
    public BatchConsumerFactory<E> inheritDecoder(BatchConsumerFactory<E> other) {
        decoderInheritParent = other;
        return this;
    }

    @JsonIgnore
    public SimpleWorker build(Environment environment,
                              AmazonKinesis kinesis,
                              AmazonDynamoDB dynamoDb,
                              String name) {
        if (environment != null && decoder == null) {
            //noinspection unchecked
            this.decoder = inferDecoder(environment.getObjectMapper());
        }
        if (consumer == null) {
            consumer = () -> event -> {
                if (event != null) {
                    LOG.info("Consumed event on " + name + ": " + event.toString());
                }
                return true;
            };
        }

        return build(environment == null ? null : environment.metrics(),
                environment == null ? null : environment.healthChecks(),
                environment == null ? null : environment.lifecycle(),
                kinesis,
                dynamoDb,
                name
        );
    }

    /**
     * Will create a {@link SimpleWorker} and attempt to start consuming.
     * If lifecycle was null, you'll have to start simple worker yourself.
     * @param metrics metrics
     * @param healthChecks healthChecks
     * @param lifeCycle lifeCycle
     * @param kinesis kinesis
     * @param dynamoDb dynamoDBClient
     * @param name name
     * @return simple worker. If lifcycle was null, you will need to start worker on your own.
     */
    @JsonIgnore
    public SimpleWorker build(MetricRegistry metrics,
                              HealthCheckRegistry healthChecks,
                              LifecycleEnvironment lifeCycle,
                              AmazonKinesis kinesis,
                              AmazonDynamoDB dynamoDb,
                              String name) {
        Preconditions.checkNotNull(decoder, "decoder cannot be null");
        Preconditions.checkNotNull(consumer, "consumer cannot be null");

        super.setupStream(kinesis);

        RecordProcessorMetrics processorMetrics = new RecordProcessorMetrics(metrics, name);
        BatchRecordProcessorFactory<E> recordProcessorFactory = new BatchRecordProcessorFactory<>(
                decoder,
                consumer,
                processorMetrics);
        SimpleWorker.Builder builder = new SimpleWorker.Builder()
                .recordProcessorFactory(recordProcessorFactory)
                .config(makeKinesisClientLibConfiguration(name))
                .kinesisClient(kinesis)
                .dynamoDBClient(dynamoDb);

        if(healthChecks != null){
            healthChecks.register(name, new StreamFailureCheck(processorMetrics, new StreamHealthCheck(kinesis, getStreamName())));
        }

        //use unbounded queue
        if (lifeCycle != null) {
            ExecutorService processorService = lifeCycle.executorService(name + "-processor-%d")
                    .build();
            builder.execService(processorService).build();
        }

        SimpleWorker worker = builder.build();

        if (lifeCycle != null){
            ExecutorService workerExecutor = lifeCycle.executorService(name + "-consumer-worker-%d")
                    .minThreads(1).maxThreads(1)
                    .build();

            lifeCycle.manage(new AbstractLifeCycle() {
                @Override
                protected void doStop() throws Exception {
                    worker.shutdown();
                }
            });

            workerExecutor.submit(worker::run);
        }
        return worker;
    }

    EventObjectMapper<E> inferDecoder(ObjectMapper objectMapper) {
        EventObjectMapper<E> decoder = null;
        Class eventClass = null;
        try {
            eventClass = (Class) ((ParameterizedType) getClass().getGenericSuperclass())
                    .getActualTypeArguments()[0];
        } catch (Exception e) {
            LOG.debug("Tried to infer event class to make default decoder, but failed", e);
        }
        if (eventClass != null) {
            //noinspection unchecked
            decoder = new EventObjectMapper<>(objectMapper, eventClass);
        } else if (decoderInheritParent != null) {
            decoder = decoderInheritParent.inferDecoder(objectMapper);
        }
        return decoder;
    }

}
