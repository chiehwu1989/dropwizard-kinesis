package io.codemonastery.dropwizard.kinesis.metric;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DeleteStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class KinesisMetricsProxyTest {

    private MetricRegistry metricRegistry;

    @Mock
    private AmazonKinesis delegate;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        metricRegistry = new MetricRegistry();
    }

    @Test
    public void addTagsToStream() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-add-tags-to-stream").getCount()).isEqualTo(0);
        metrics.addTagsToStream(null);
        assertThat(metricRegistry.timer("foo-add-tags-to-stream").getCount()).isEqualTo(1);
    }

    @Test
    public void createStreamTimer() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-create-stream").getCount()).isEqualTo(0);
        metrics.createStream(null);
        metrics.createStream(null, null);
        assertThat(metricRegistry.timer("foo-create-stream").getCount()).isEqualTo(2);
    }

    @Test
    public void decreaseStreamRetentionPeriod() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-decrease-stream-retention-period").getCount()).isEqualTo(0);
        metrics.decreaseStreamRetentionPeriod(null);
        assertThat(metricRegistry.timer("foo-decrease-stream-retention-period").getCount()).isEqualTo(1);
    }

    @Test
    public void deleteStream() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-delete-stream").getCount()).isEqualTo(0);
        metrics.deleteStream((DeleteStreamRequest) null);
        metrics.deleteStream((String) null);
        assertThat(metricRegistry.timer("foo-delete-stream").getCount()).isEqualTo(2);
    }

    @Test
    public void describeStream() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-describe-stream").getCount()).isEqualTo(0);
        metrics.describeStream((DescribeStreamRequest) null);
        metrics.describeStream((String) null);
        metrics.describeStream((String) null);
        metrics.describeStream(null, null);
        metrics.describeStream(null, null, null);
        assertThat(metricRegistry.timer("foo-describe-stream").getCount()).isEqualTo(5);
    }

    @Test
    public void getRecords() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-get-records").getCount()).isEqualTo(0);
        metrics.getRecords(null);
        assertThat(metricRegistry.timer("foo-get-records").getCount()).isEqualTo(1);
    }

    @Test
    public void getShardIterator() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-get-shard-iterator").getCount()).isEqualTo(0);
        metrics.getShardIterator(null);
        metrics.getShardIterator(null, null, null);
        metrics.getShardIterator(null, null, null, null);
        assertThat(metricRegistry.timer("foo-get-shard-iterator").getCount()).isEqualTo(3);
    }

    @Test
    public void increaseStreamRetentionPeriod() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-increase-stream-retention-period").getCount()).isEqualTo(0);
        metrics.increaseStreamRetentionPeriod(null);
        assertThat(metricRegistry.timer("foo-increase-stream-retention-period").getCount()).isEqualTo(1);
    }

    @Test
    public void listStreams() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-list-streams").getCount()).isEqualTo(0);
        metrics.listStreams();
        metrics.listStreams((ListStreamsRequest) null);
        metrics.listStreams((String) null);
        metrics.listStreams(null, null);
        assertThat(metricRegistry.timer("foo-list-streams").getCount()).isEqualTo(4);
    }

    @Test
    public void listTagsForStreams() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-list-tags-for-stream").getCount()).isEqualTo(0);
        metrics.listTagsForStream(null);
        assertThat(metricRegistry.timer("foo-list-tags-for-stream").getCount()).isEqualTo(1);
    }

    @Test
    public void mergeShards() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-merge-shards").getCount()).isEqualTo(0);
        metrics.mergeShards(null);
        metrics.mergeShards(null, null, null);
        assertThat(metricRegistry.timer("foo-merge-shards").getCount()).isEqualTo(2);
    }

    @Test
    public void putRecord() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-put-record").getCount()).isEqualTo(0);
        metrics.putRecord(null);
        metrics.putRecord(null, null, null);
        metrics.putRecord(null, null, null, null);
        assertThat(metricRegistry.timer("foo-put-record").getCount()).isEqualTo(3);
    }

    @Test
    public void putRecords() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-put-records").getCount()).isEqualTo(0);
        metrics.putRecords(null);
        assertThat(metricRegistry.timer("foo-put-records").getCount()).isEqualTo(1);
    }

    @Test
    public void removeTagsFromStreamTimer() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-remove-tags-from-stream").getCount()).isEqualTo(0);
        metrics.removeTagsFromStream(null);
        assertThat(metricRegistry.timer("foo-remove-tags-from-stream").getCount()).isEqualTo(1);
    }

    @Test
    public void splitShard() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");

        assertThat(metricRegistry.timer("foo-split-shard").getCount()).isEqualTo(0);
        metrics.splitShard(null);
        metrics.splitShard(null, null, null);
        assertThat(metricRegistry.timer("foo-split-shard").getCount()).isEqualTo(2);
    }

    @Test
    public void shutdown() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");
        verify(delegate, never()).shutdown();
        metrics.shutdown();
        verify(delegate, times(1)).shutdown();
    }

    @Test
    public void getCachedResponseMetadata() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");
        verify(delegate, never()).getCachedResponseMetadata(any());
        metrics.getCachedResponseMetadata(null);
        verify(delegate, times(1)).getCachedResponseMetadata(any());
    }

    @Test
    public void setEndpoint() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");
        verify(delegate, never()).setEndpoint(any());
        metrics.setEndpoint(null);
        verify(delegate, times(1)).setEndpoint(any());
    }

    @Test
    public void setRegion() throws Exception {
        KinesisMetricsProxy metrics = new KinesisMetricsProxy(delegate, metricRegistry, "foo");
        verify(delegate, never()).setRegion(any());
        metrics.setRegion(null);
        verify(delegate, times(1)).setRegion(any());
    }
}
