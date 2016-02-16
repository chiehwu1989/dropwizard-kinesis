package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import io.codemonastery.dropwizard.kinesis.StreamConfiguration;
import io.dropwizard.util.Duration;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration.*;


public class KinesisClientLibConfig extends StreamConfiguration {

    private String applicationName;

    private String workerId;

    @NotNull
    private  InitialPositionInStream initialPositionInStream = InitialPositionInStream.LATEST;

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
        ClientConfiguration unusedConfig = new ClientConfiguration();
        //noinspection ConstantConditions
        return new KinesisClientLibConfiguration(
                Optional.fromNullable(applicationName).or(name),
                getStreamName(),
                null,
                initialPositionInStream,
                null,
                null,
                null,
                failOverTime.toMilliseconds(),
                Optional.fromNullable(workerId).or(name),
                maxRecords,
                idleTimeBetweenReads.toMilliseconds(),
                !callIfEmpty,
                parentShardPollInterval.toMilliseconds(),
                shardSyncInterval.toMilliseconds(),
                cleanupLeasesOnShardCompletion,
                unusedConfig,
                unusedConfig,
                unusedConfig,
                taskBackoffTime.toMilliseconds(),
                DEFAULT_METRICS_BUFFER_TIME_MILLIS,
                DEFAULT_METRICS_MAX_QUEUE_SIZE,
                validateSequenceNumberBeforeCheckpoint,
                null
        );
    }
}
