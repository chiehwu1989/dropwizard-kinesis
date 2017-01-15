package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.StreamConfiguration;
import io.dropwizard.util.Duration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.UUID;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration.*;


public class KinesisClientLibConfig extends StreamConfiguration {

    private String applicationName;

    private String workerId;

    @NotNull
    private InitialPositionInStream initialPositionInStream = InitialPositionInStream.LATEST;

    @Valid
    @NotNull
    private Duration failOverTime = Duration.milliseconds(DEFAULT_FAILOVER_TIME_MILLIS);

    @Min(1)
    private int maxRecords = DEFAULT_MAX_RECORDS;

    @Valid
    @NotNull
    private Duration idleTimeBetweenReads = Duration.milliseconds(DEFAULT_IDLETIME_BETWEEN_READS_MILLIS);


    private boolean callIfEmpty = false;

    @Valid
    @NotNull
    private Duration parentShardPollInterval = Duration.milliseconds(DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS);

    @Valid
    @NotNull
    private Duration shardSyncInterval = Duration.milliseconds(DEFAULT_SHARD_SYNC_INTERVAL_MILLIS);

    private boolean cleanupLeasesOnShardCompletion = DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION;

    @Valid
    @NotNull
    private Duration taskBackoffTime = Duration.milliseconds(DEFAULT_TASK_BACKOFF_TIME_MILLIS);

    private boolean validateSequenceNumberBeforeCheckpoint = DEFAULT_VALIDATE_SEQUENCE_NUMBER_BEFORE_CHECKPOINTING;

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
    public InitialPositionInStream getInitialPositionInStream() {
        return initialPositionInStream;
    }

    @JsonProperty
    public void setInitialPositionInStream(InitialPositionInStream initialPositionInStream) {
        this.initialPositionInStream = initialPositionInStream;
    }

    @JsonProperty
    public Duration getFailOverTime() {
        return failOverTime;
    }

    @JsonProperty
    public void setFailOverTime(Duration failOverTime) {
        this.failOverTime = failOverTime;
    }

    @JsonProperty
    public int getMaxRecords() {
        return maxRecords;
    }

    @JsonProperty
    public void setMaxRecords(int maxRecords) {
        this.maxRecords = maxRecords;
    }

    @JsonProperty
    public Duration getIdleTimeBetweenReads() {
        return idleTimeBetweenReads;
    }

    @JsonProperty
    public void setIdleTimeBetweenReads(Duration idleTimeBetweenReads) {
        this.idleTimeBetweenReads = idleTimeBetweenReads;
    }

    @JsonProperty
    public boolean isCallIfEmpty() {
        return callIfEmpty;
    }

    @JsonProperty
    public void setCallIfEmpty(boolean callIfEmpty) {
        this.callIfEmpty = callIfEmpty;
    }

    @JsonProperty
    public Duration getParentShardPollInterval() {
        return parentShardPollInterval;
    }

    @JsonProperty
    public void setParentShardPollInterval(Duration parentShardPollInterval) {
        this.parentShardPollInterval = parentShardPollInterval;
    }

    @JsonProperty
    public Duration getShardSyncInterval() {
        return shardSyncInterval;
    }

    @JsonProperty
    public void setShardSyncInterval(Duration shardSyncInterval) {
        this.shardSyncInterval = shardSyncInterval;
    }

    @JsonProperty
    public boolean isCleanupLeasesOnShardCompletion() {
        return cleanupLeasesOnShardCompletion;
    }

    @JsonProperty
    public void setCleanupLeasesOnShardCompletion(boolean cleanupLeasesOnShardCompletion) {
        this.cleanupLeasesOnShardCompletion = cleanupLeasesOnShardCompletion;
    }

    @JsonProperty
    public Duration getTaskBackoffTime() {
        return taskBackoffTime;
    }

    @JsonProperty
    public void setTaskBackoffTime(Duration taskBackoffTime) {
        this.taskBackoffTime = taskBackoffTime;
    }

    @JsonProperty
    public boolean isValidateSequenceNumberBeforeCheckpoint() {
        return validateSequenceNumberBeforeCheckpoint;
    }

    @JsonProperty
    public void setValidateSequenceNumberBeforeCheckpoint(boolean validateSequenceNumberBeforeCheckpoint) {
        this.validateSequenceNumberBeforeCheckpoint = validateSequenceNumberBeforeCheckpoint;
    }

    KinesisClientLibConfiguration makeKinesisClientLibConfiguration(String name) {
        Preconditions.checkNotNull(getStreamName(), "streamName cannot be null");
        Preconditions.checkNotNull(initialPositionInStream, "initialPositionInStream cannot be null");
        Preconditions.checkNotNull(failOverTime, "failOverTime cannot be null");
        Preconditions.checkNotNull(idleTimeBetweenReads, "idleTimeBetweenReads cannot be null");
        Preconditions.checkNotNull(parentShardPollInterval, "parentShardPollInterval cannot be null");
        Preconditions.checkNotNull(shardSyncInterval, "shardSyncInterval cannot be null");
        Preconditions.checkNotNull(taskBackoffTime, "taskBackoffTime cannot be null");

        ClientConfiguration unusedConfig = new ClientConfiguration();
        //noinspection ConstantConditions
        return new KinesisClientLibConfiguration(
                Optional.fromNullable(getApplicationName()).or(name),
                getStreamName(),
                null,
                getInitialPositionInStream(),
                null,
                null,
                null,
                getFailOverTime().toMilliseconds(),
                Optional.fromNullable(getWorkerId()).or(name + "-" + UUID.randomUUID()),
                getMaxRecords(),
                getIdleTimeBetweenReads().toMilliseconds(),
                isCallIfEmpty(),
                getParentShardPollInterval().toMilliseconds(),
                getShardSyncInterval().toMilliseconds(),
                isCleanupLeasesOnShardCompletion(),
                unusedConfig,
                unusedConfig,
                unusedConfig,
                getTaskBackoffTime().toMilliseconds(),
                DEFAULT_METRICS_BUFFER_TIME_MILLIS,
                DEFAULT_METRICS_MAX_QUEUE_SIZE,
                isValidateSequenceNumberBeforeCheckpoint(),
                null
        );
    }
}
