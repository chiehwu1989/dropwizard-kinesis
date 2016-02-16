package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleWorker;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import io.codemonastery.dropwizard.kinesis.EventDecoder;
import io.codemonastery.dropwizard.kinesis.EventObjectMapper;
import io.codemonastery.dropwizard.kinesis.StreamConfiguration;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration.*;

public class ConsumerFactory<E> extends StreamConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerFactory.class);

    private EventDecoder<E> decoder;

    private Supplier<EventConsumer<E>> eventConsumerFactory;

    @NotEmpty
    private String applicationName;

    @NotEmpty
    private String workerId;

    @Min(1024)
    @Max(5 * 1024 * 1024)
    private int bufferSize = 1024 * 1024 * 5;

    @Min(1)
    @Max(500)
    private int maxElements = 500;

    @Min(1)
    private int flushPeriodSeconds = 60;

    private static InitialPositionInStream initialPositionInStream =
            InitialPositionInStream.LATEST;

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

    @JsonProperty
    public String getApplicationName() {
        return applicationName;
    }

    @JsonProperty
    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    @JsonProperty
    public String getWorkerId() {
        return workerId;
    }

    @JsonProperty
    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    @JsonProperty
    public int getBufferSize() {
        return bufferSize;
    }

    @JsonProperty
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @JsonProperty
    public int getMaxElements() {
        return maxElements;
    }

    @JsonProperty
    public void setMaxElements(int maxElements) {
        this.maxElements = maxElements;
    }

    @JsonProperty
    public int getFlushPeriodSeconds() {
        return flushPeriodSeconds;
    }

    @JsonProperty
    public void setFlushPeriodSeconds(int flushPeriodSeconds) {
        this.flushPeriodSeconds = flushPeriodSeconds;
    }

    @JsonProperty
    public static InitialPositionInStream getInitialPositionInStream() {
        return initialPositionInStream;
    }

    @JsonProperty
    public static void setInitialPositionInStream(InitialPositionInStream initialPositionInStream) {
        ConsumerFactory.initialPositionInStream = initialPositionInStream;
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

        final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
                Optional.fromNullable(applicationName).or(name),
                getStreamName(),
                null,
                initialPositionInStream,
                null,
                null,
                null,
                DEFAULT_FAILOVER_TIME_MILLIS,
                Optional.fromNullable(workerId).or(name),
                DEFAULT_MAX_RECORDS,
                DEFAULT_IDLETIME_BETWEEN_READS_MILLIS,
                DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST,
                DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS,
                DEFAULT_SHARD_SYNC_INTERVAL_MILLIS,
                DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION,
                null,
                null,
                null,
                DEFAULT_TASK_BACKOFF_TIME_MILLIS,
                DEFAULT_METRICS_BUFFER_TIME_MILLIS,
                DEFAULT_METRICS_MAX_QUEUE_SIZE,
                DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING,
                null
        );


        SimpleWorker.Builder builder = new SimpleWorker.Builder()
                .recordProcessorFactory(() -> new RecordProcessor<>(decoder, eventConsumerFactory.get()))
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
