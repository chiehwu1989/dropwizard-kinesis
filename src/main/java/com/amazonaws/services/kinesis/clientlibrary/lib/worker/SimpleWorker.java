package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.ICheckpoint;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.leases.exceptions.LeasingException;
import com.amazonaws.services.kinesis.leases.impl.KinesisClientLeaseManager;
import com.amazonaws.services.kinesis.metrics.impl.CWMetricsFactory;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.IMetricsFactory;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Simpler implementation of com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
 * that does not try to log to cloudwatch (yet).
 */
public class SimpleWorker implements Runnable {

    private static final int MAX_INITIALIZATION_ATTEMPTS = 20;
    private static final Log LOG = LogFactory.getLog(SimpleWorker.class);
    private WorkerLog wlog = new WorkerLog();

    private final IRecordProcessorFactory recordProcessorFactory;
    private final StreamConfig streamConfig;
    private final InitialPositionInStream initialPosition;
    private final ICheckpoint checkpointTracker;
    private final long idleTimeInMilliseconds;
    private final long parentShardPollIntervalMillis;
    private final ExecutorService executorService;
    private final IMetricsFactory metricsFactory;
    private final long taskBackoffTimeMillis;

    private final KinesisClientLibLeaseCoordinator leaseCoordinator;
    private final ShardSyncTaskManager controlServer;

    private volatile boolean shutdown;

    private final ConcurrentMap<ShardInfo, ShardConsumer> shardInfoShardConsumerMap =
            new ConcurrentHashMap<>();
    private final boolean cleanupLeasesUponShardCompletion;


    // NOTE: This has package level access solely for testing
    // CHECKSTYLE:IGNORE ParameterNumber FOR NEXT 10 LINES
    SimpleWorker(IRecordProcessorFactory recordProcessorFactory,
                 StreamConfig streamConfig,
                 InitialPositionInStream initialPositionInStream,
                 long parentShardPollIntervalMillis,
                 long shardSyncIdleTimeMillis,
                 boolean cleanupLeasesUponShardCompletion,
                 ICheckpoint checkpoint,
                 KinesisClientLibLeaseCoordinator leaseCoordinator,
                 ExecutorService execService,
                 IMetricsFactory metricsFactory,
                 long taskBackoffTimeMillis) {
        this.recordProcessorFactory = recordProcessorFactory;
        this.streamConfig = streamConfig;
        this.initialPosition = initialPositionInStream;
        this.parentShardPollIntervalMillis = parentShardPollIntervalMillis;
        this.cleanupLeasesUponShardCompletion = cleanupLeasesUponShardCompletion;
        this.checkpointTracker = checkpoint != null ? checkpoint : leaseCoordinator;
        this.idleTimeInMilliseconds = streamConfig.getIdleTimeInMilliseconds();
        this.executorService = execService;
        this.leaseCoordinator = leaseCoordinator;
        this.metricsFactory = metricsFactory;
        this.controlServer =
                new ShardSyncTaskManager(streamConfig.getStreamProxy(),
                        leaseCoordinator.getLeaseManager(),
                        initialPositionInStream,
                        cleanupLeasesUponShardCompletion,
                        shardSyncIdleTimeMillis,
                        metricsFactory,
                        executorService);
        this.taskBackoffTimeMillis = taskBackoffTimeMillis;
    }

    public void run() {
        try {
            initialize();
            LOG.info("Initialization complete. Starting worker loop.");
        } catch (RuntimeException e1) {
            LOG.error("Unable to initialize after " + MAX_INITIALIZATION_ATTEMPTS + " attempts. Shutting down.", e1);
            shutdown();
        }

        while (!shutdown) {
            try {
                boolean foundCompletedShard = false;
                Set<ShardInfo> assignedShards = new HashSet<>();
                for (ShardInfo shardInfo : getShardInfoForAssignments()) {
                    ShardConsumer shardConsumer = createOrGetShardConsumer(shardInfo, recordProcessorFactory);
                    if (shardConsumer.isShutdown()
                            && shardConsumer.getShutdownReason().equals(ShutdownReason.TERMINATE)) {
                        foundCompletedShard = true;
                    } else {
                        shardConsumer.consumeShard();
                    }
                    assignedShards.add(shardInfo);
                }

                if (foundCompletedShard) {
                    controlServer.syncShardAndLeaseInfo(null);
                }

                // clean up shard consumers for unassigned shards
                cleanupShardConsumers(assignedShards);

                wlog.info("Sleeping ...");
                Thread.sleep(idleTimeInMilliseconds);
            } catch (Exception e) {
                LOG.error(String.format("Worker.run caught exception, sleeping for %s milli seconds!",
                                String.valueOf(idleTimeInMilliseconds)),
                        e);
                try {
                    Thread.sleep(idleTimeInMilliseconds);
                } catch (InterruptedException ex) {
                    LOG.info("Worker: sleep interrupted after catching exception ", ex);
                }
            }
            wlog.resetInfoLogging();
        }

        LOG.info("Stopping LeaseCoordinator.");
        leaseCoordinator.stop();
    }

    private void initialize() {
        boolean isDone = false;
        Exception lastException = null;

        for (int i = 0; (!isDone) && (i < MAX_INITIALIZATION_ATTEMPTS); i++) {
            try {
                LOG.info("Initialization attempt " + (i + 1));
                LOG.info("Initializing LeaseCoordinator");
                leaseCoordinator.initialize();

                LOG.info("Syncing Kinesis shard info");
                ShardSyncTask shardSyncTask =
                        new ShardSyncTask(streamConfig.getStreamProxy(),
                                leaseCoordinator.getLeaseManager(),
                                initialPosition,
                                cleanupLeasesUponShardCompletion,
                                0L);
                TaskResult result = new MetricsCollectingTaskDecorator(shardSyncTask, metricsFactory).call();

                //noinspection ThrowableResultOfMethodCallIgnored
                if (result.getException() == null) {
                    if (!leaseCoordinator.isRunning()) {
                        LOG.info("Starting LeaseCoordinator");
                        leaseCoordinator.start();
                    } else {
                        LOG.info("LeaseCoordinator is already running. No need to start it.");
                    }
                    isDone = true;
                } else {
                    lastException = result.getException();
                }
            } catch (LeasingException e) {
                LOG.error("Caught exception when initializing LeaseCoordinator", e);
                lastException = e;
            } catch (Exception e) {
                lastException = e;
            }

            try {
                Thread.sleep(parentShardPollIntervalMillis);
            } catch (InterruptedException e) {
                LOG.debug("Sleep interrupted while initializing worker.");
            }
        }

        if (!isDone) {
            throw new RuntimeException(lastException);
        }
    }

    void cleanupShardConsumers(Set<ShardInfo> assignedShards) {
        // Shutdown the consumer since we are not longer responsible for
// the shard.
        shardInfoShardConsumerMap.keySet().stream().filter(shard -> !assignedShards.contains(shard)).forEach(shard -> {
            // Shutdown the consumer since we are not longer responsible for
            // the shard.
            boolean isShutdown = shardInfoShardConsumerMap.get(shard).beginShutdown();
            if (isShutdown) {
                shardInfoShardConsumerMap.remove(shard);
            }
        });
    }

    private List<ShardInfo> getShardInfoForAssignments() {
        List<ShardInfo> assignedStreamShards = leaseCoordinator.getCurrentAssignments();

        if ((assignedStreamShards != null) && (!assignedStreamShards.isEmpty())) {
            if (wlog.isInfoEnabled()) {
                StringBuilder builder = new StringBuilder();
                boolean firstItem = true;
                for (ShardInfo shardInfo : assignedStreamShards) {
                    if (!firstItem) {
                        builder.append(", ");
                    }
                    builder.append(shardInfo.getShardId());
                    firstItem = false;
                }
                wlog.info("Current stream shard assignments: " + builder.toString());
            }
        } else {
            wlog.info("No activities assigned");
        }

        return assignedStreamShards;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    ShardConsumer createOrGetShardConsumer(ShardInfo shardInfo, IRecordProcessorFactory factory) {
        synchronized (shardInfoShardConsumerMap) {
            ShardConsumer consumer = shardInfoShardConsumerMap.get(shardInfo);
            // Instantiate a new consumer if we don't have one, or the one we
            // had was from an earlier
            // lease instance (and was shutdown). Don't need to create another
            // one if the shard has been
            // completely processed (shutdown reason terminate).
            if ((consumer == null)
                    || (consumer.isShutdown() && consumer.getShutdownReason().equals(ShutdownReason.ZOMBIE))) {
                IRecordProcessor recordProcessor = factory.createProcessor();

                consumer =
                        new ShardConsumer(shardInfo,
                                streamConfig,
                                checkpointTracker,
                                recordProcessor,
                                leaseCoordinator.getLeaseManager(),
                                parentShardPollIntervalMillis,
                                cleanupLeasesUponShardCompletion,
                                executorService,
                                metricsFactory,
                                taskBackoffTimeMillis);
                shardInfoShardConsumerMap.put(shardInfo, consumer);
                wlog.infoForce("Created new shardConsumer for : " + shardInfo);
            }
            return consumer;
        }
    }

    private static class WorkerLog {

        private long reportIntervalMillis = TimeUnit.MINUTES.toMillis(1);
        private long nextReportTime = System.currentTimeMillis() + reportIntervalMillis;
        private boolean infoReporting;

        private WorkerLog() {

        }

        @SuppressWarnings("unused")
        public void debug(Object message, Throwable t) {
            LOG.debug(message, t);
        }

        public void info(Object message) {
            if (this.isInfoEnabled()) {
                LOG.info(message);
            }
        }

        public void infoForce(Object message) {
            LOG.info(message);
        }

        @SuppressWarnings("unused")
        public void warn(Object message) {
            LOG.warn(message);
        }

        @SuppressWarnings("unused")
        public void error(Object message, Throwable t) {
            LOG.error(message, t);
        }

        private boolean isInfoEnabled() {
            return infoReporting;
        }

        private void resetInfoLogging() {
            if (infoReporting) {
                // We just logged at INFO level for a pass through worker loop
                if (LOG.isInfoEnabled()) {
                    infoReporting = false;
                    nextReportTime = System.currentTimeMillis() + reportIntervalMillis;
                } // else is DEBUG or TRACE so leave reporting true
            } else if (nextReportTime <= System.currentTimeMillis()) {
                infoReporting = true;
            }
        }
    }

    private static IMetricsFactory getMetricsFactory(
            AmazonCloudWatch cloudWatchClient, KinesisClientLibConfiguration config) {
        return config.getMetricsLevel() == MetricsLevel.NONE
                ? new NullMetricsFactory() : new CWMetricsFactory(
                cloudWatchClient,
                config.getApplicationName(),
                config.getMetricsBufferTimeMillis(),
                config.getMetricsMaxQueueSize(),
                config.getMetricsLevel(),
                config.getMetricsEnabledDimensions());
    }

    public static class Builder {

        private IRecordProcessorFactory recordProcessorFactory;
        private KinesisClientLibConfiguration config;
        private AmazonKinesis kinesisClient;
        private AmazonDynamoDB dynamoDBClient;
        private IMetricsFactory metricsFactory;
        private ExecutorService execService;

        public Builder() {
        }

        public Builder recordProcessorFactory(IRecordProcessorFactory recordProcessorFactory) {
            this.recordProcessorFactory = recordProcessorFactory;
            return this;
        }

        public Builder config(KinesisClientLibConfiguration config) {
            this.config = config;
            return this;
        }

        public Builder kinesisClient(AmazonKinesis kinesisClient) {
            this.kinesisClient = kinesisClient;
            return this;
        }

        public Builder dynamoDBClient(AmazonDynamoDB dynamoDBClient) {
            this.dynamoDBClient = dynamoDBClient;
            return this;
        }

        public Builder metricsFactory(IMetricsFactory metricsFactory) {
            this.metricsFactory = metricsFactory;
            return this;
        }

        public Builder execService(ExecutorService execService) {
            this.execService = execService;
            return this;
        }

        // CHECKSTYLE:OFF CyclomaticComplexity
        // CHECKSTYLE:OFF NPathComplexity
        public SimpleWorker build() {
            if (config == null) {
                throw new IllegalArgumentException(
                        "Kinesis Client Library configuration needs to be provided to build Worker");
            }
            if (recordProcessorFactory == null) {
                throw new IllegalArgumentException(
                        "A Record Processor Factory needs to be provided to build Worker");
            }
            if (execService == null) {
                execService = Executors.newCachedThreadPool();
            }
            if (kinesisClient == null) {
                kinesisClient = new AmazonKinesisClient(config.getKinesisCredentialsProvider(),
                        config.getKinesisClientConfiguration());
            }
            if (dynamoDBClient == null) {
                dynamoDBClient = new AmazonDynamoDBClient(config.getDynamoDBCredentialsProvider(),
                        config.getDynamoDBClientConfiguration());
            }
            // If a region name was explicitly specified, use it as the region for Amazon Kinesis and Amazon DynamoDB.
            if (config.getRegionName() != null) {
                Region region = RegionUtils.getRegion(config.getRegionName());
                LOG.debug("The region of Amazon CloudWatch client has been set to " + config.getRegionName());
                kinesisClient.setRegion(region);
                LOG.debug("The region of Amazon Kinesis client has been set to " + config.getRegionName());
                dynamoDBClient.setRegion(region);
                LOG.debug("The region of Amazon DynamoDB client has been set to " + config.getRegionName());
            }
            // If a kinesis endpoint was explicitly specified, use it to set the region of kinesis.
            if (config.getKinesisEndpoint() != null) {
                kinesisClient.setEndpoint(config.getKinesisEndpoint());
                if (config.getRegionName() != null) {
                    LOG.warn("Received configuration for both region name as " + config.getRegionName()
                            + ", and Amazon Kinesis endpoint as " + config.getKinesisEndpoint()
                            + ". Amazon Kinesis endpoint will overwrite region name.");
                    LOG.debug("The region of Amazon Kinesis client has been overwritten to "
                            + config.getKinesisEndpoint());
                } else  {
                    LOG.debug("The region of Amazon Kinesis client has been set to " + config.getKinesisEndpoint());
                }
            }
            if(metricsFactory == null){
                metricsFactory = new NullMetricsFactory();
            }
            return new SimpleWorker(
                    recordProcessorFactory,
                    new StreamConfig(
                            new SimpleKinesisProxy(
                                    kinesisClient,
                                    config.getStreamName()
                            ),
                            config.getMaxRecords(),
                            config.getIdleTimeBetweenReadsInMillis(),
                            config.shouldCallProcessRecordsEvenForEmptyRecordList(),
                            config.shouldValidateSequenceNumberBeforeCheckpointing(),
                            config.getInitialPositionInStream()),
                    config.getInitialPositionInStream(),
                    config.getParentShardPollIntervalMillis(),
                    config.getShardSyncIntervalMillis(),
                    config.shouldCleanupLeasesUponShardCompletion(),
                    null,
                    new KinesisClientLibLeaseCoordinator(new KinesisClientLeaseManager(config.getApplicationName(),
                            dynamoDBClient),
                            config.getWorkerIdentifier(),
                            config.getFailoverTimeMillis(),
                            config.getEpsilonMillis(),
                            metricsFactory),
                    execService,
                    metricsFactory,
                    config.getTaskBackoffTimeMillis());
        }

    }
}
