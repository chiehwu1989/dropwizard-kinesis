package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import io.dropwizard.util.Duration;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class KinesisClientLibConfigTest {

    @Test
    public void inferApplicationNameAndWorkerId() throws Exception {
        KinesisClientLibConfiguration otherConfig = config()
                .makeKinesisClientLibConfiguration("foo");

        assertThat(otherConfig.getApplicationName()).isEqualTo("foo");
        assertThat(otherConfig.getWorkerIdentifier()).startsWith("foo-");
    }

    @Test
    public void applicationName() throws Exception {
        KinesisClientLibConfig config = config();
        config.setApplicationName("app");
        assertThat(config.getApplicationName()).isEqualTo("app");

        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.getApplicationName()).isEqualTo("app");
    }

    @Test
    public void workerId() throws Exception {
        KinesisClientLibConfig config = config();
        config.setWorkerId("worker");
        assertThat(config.getWorkerId()).isEqualTo("worker");

        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.getWorkerIdentifier()).isEqualTo("worker");
    }

    @Test
    public void initialPositionInStream() throws Exception {
        KinesisClientLibConfig config = config();
        config.setInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.getInitialPositionInStream()).isEqualTo(InitialPositionInStream.TRIM_HORIZON);
    }

    @Test
    public void failOverTime() throws Exception {
        KinesisClientLibConfig config = config();
        config.setFailOverTime(Duration.days(1));
        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.getFailoverTimeMillis()).isEqualTo(config.getFailOverTime().toMilliseconds());
    }

    @Test
    public void maxRecords() throws Exception {
        KinesisClientLibConfig config = config();
        config.setMaxRecords(777);
        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.getMaxRecords()).isEqualTo(config.getMaxRecords());
    }

    @Test
    public void idleTimeBetweenReads() throws Exception {
        KinesisClientLibConfig config = config();
        config.setIdleTimeBetweenReads(Duration.days(2));
        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.getIdleTimeBetweenReadsInMillis()).isEqualTo(config.getIdleTimeBetweenReads().toMilliseconds());
    }

    @Test
    public void callIfEmpty() throws Exception {
        KinesisClientLibConfig config = config();
        config.setCallIfEmpty(!config.isCallIfEmpty());
        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.shouldCallProcessRecordsEvenForEmptyRecordList()).isEqualTo(config.isCallIfEmpty());
    }

    @Test
    public void parentPollShardInterval() throws Exception {
        KinesisClientLibConfig config = config();
        config.setParentShardPollInterval(Duration.days(3));
        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.getParentShardPollIntervalMillis()).isEqualTo(config.getParentShardPollInterval().toMilliseconds());
    }

    @Test
    public void shardSyncInterval() throws Exception {
        KinesisClientLibConfig config = config();
        config.setShardSyncInterval(Duration.days(4));
        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.getShardSyncIntervalMillis()).isEqualTo(config.getShardSyncInterval().toMilliseconds());
    }

    @Test
    public void cleanupLeasesUponShardCompletion() throws Exception {
        KinesisClientLibConfig config = config();
        config.setCleanupLeasesOnShardCompletion(!config.isCleanupLeasesOnShardCompletion());
        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.shouldCleanupLeasesUponShardCompletion()).isEqualTo(config.isCleanupLeasesOnShardCompletion());
    }

    @Test
    public void taskBackoffTime() throws Exception {
        KinesisClientLibConfig config = config();
        config.setTaskBackoffTime(Duration.days(5));
        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.getTaskBackoffTimeMillis()).isEqualTo(config.getTaskBackoffTime().toMilliseconds());
    }

    @Test
    public void validateSequenceNumberBeforeCheckpoint() throws Exception {
        KinesisClientLibConfig config = config();
        config.setValidateSequenceNumberBeforeCheckpoint(!config.isValidateSequenceNumberBeforeCheckpoint());
        KinesisClientLibConfiguration otherConfig = config.makeKinesisClientLibConfiguration("foo");
        assertThat(otherConfig.shouldValidateSequenceNumberBeforeCheckpointing()).isEqualTo(config.isValidateSequenceNumberBeforeCheckpoint());
    }

    private KinesisClientLibConfig config() {
        KinesisClientLibConfig config = new KinesisClientLibConfig();
        config.setStreamName("xyz");
        return config;
    }
}
