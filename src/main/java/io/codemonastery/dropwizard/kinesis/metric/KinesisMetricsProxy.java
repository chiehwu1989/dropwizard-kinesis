package io.codemonastery.dropwizard.kinesis.metric;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

@SuppressWarnings("unused")
public class KinesisMetricsProxy implements AmazonKinesis {

    private final AmazonKinesis delegate;

    private final Timer addTagsToStreamTimer;
    private final Timer createStreamTimer;
    private final Timer decreaseStreamRetentionPeriodTimer;
    private final Timer getRecordsTimer;
    private final Timer getShardIteratorTimer;
    private final Timer increaseStreamRetentionPeriodTimer;
    private final Timer listStreamsTimer;
    private final Timer listTagsForStreamTimer;
    private final Timer mergeShardsTimer;
    private final Timer putRecordTimer;
    private final Timer putRecordsTimer;
    private final Timer removeTagsFromStreamTimer;
    private final Timer splitShardTimer;
    private final Timer deleteStreamTimer;
    private final Timer describeStreamTimer;

    public KinesisMetricsProxy(AmazonKinesis delegate, MetricRegistry metrics, String name) {
        Preconditions.checkNotNull(delegate, "delegate cannot be null");
        this.delegate = delegate;
        addTagsToStreamTimer = metrics.timer(name + "-add-tags-to-stream");
        createStreamTimer = metrics.timer(name + "-create-stream");
        decreaseStreamRetentionPeriodTimer = metrics.timer(name + "-decrease-stream-retention-period");
        deleteStreamTimer = metrics.timer(name + "-delete-stream");
        describeStreamTimer = metrics.timer(name + "-describe-stream");
        getRecordsTimer = metrics.timer(name + "-put-record");
        getShardIteratorTimer = metrics.timer(name + "-get-shard-iterator");
        increaseStreamRetentionPeriodTimer = metrics.timer(name + "-increase-stream-retention-period");
        listStreamsTimer = metrics.timer(name + "-list-streams");
        listTagsForStreamTimer = metrics.timer(name + "-list-tags-for-stream");
        mergeShardsTimer = metrics.timer(name + "-merge-shards");
        putRecordTimer = metrics.timer(name + "-get-records");
        putRecordsTimer = metrics.timer(name + "-put-record");
        removeTagsFromStreamTimer = metrics.timer(name + "-remove-tags-from-stream");
        splitShardTimer = metrics.timer(name + "-split-shard");
    }

    @Override
    public void setEndpoint(String endpoint) {
        delegate.setEndpoint(endpoint);
    }

    @Override
    public void setRegion(Region region) {
        delegate.setRegion(region);
    }

    @Override
    public void addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) {
        try (Timer.Context time = addTagsToStreamTimer.time()) {
            delegate.addTagsToStream(addTagsToStreamRequest);
        }
    }

    @Override
    public void createStream(CreateStreamRequest createStreamRequest) {
        try (Timer.Context time = createStreamTimer.time()) {
            delegate.createStream(createStreamRequest);
        }
    }

    @Override
    public void createStream(String streamName, Integer shardCount) {
        try (Timer.Context time = createStreamTimer.time()) {
            delegate.createStream(streamName, shardCount);
        }
    }

    @Override
    public void decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
        try (Timer.Context time = decreaseStreamRetentionPeriodTimer.time()) {
            delegate.decreaseStreamRetentionPeriod(decreaseStreamRetentionPeriodRequest);
        }
    }

    @Override
    public void deleteStream(DeleteStreamRequest deleteStreamRequest) {
        try (Timer.Context time = deleteStreamTimer.time()) {
            delegate.deleteStream(deleteStreamRequest);
        }
    }

    @Override
    public void deleteStream(String streamName) {
        try (Timer.Context time = deleteStreamTimer.time()) {
            delegate.deleteStream(streamName);
        }
    }

    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) {
        try (Timer.Context time = describeStreamTimer.time()) {
            return delegate.describeStream(describeStreamRequest);
        }
    }

    @Override
    public DescribeStreamResult describeStream(String streamName) {
        try (Timer.Context time = describeStreamTimer.time()) {
            return delegate.describeStream(streamName);
        }
    }

    @Override
    public DescribeStreamResult describeStream(String streamName, String exclusiveStartShardId) {
        try (Timer.Context time = describeStreamTimer.time()) {
            return delegate.describeStream(streamName, exclusiveStartShardId);
        }
    }

    @Override
    public DescribeStreamResult describeStream(String streamName, Integer limit, String exclusiveStartShardId) {
        try (Timer.Context time = describeStreamTimer.time()) {
            return delegate.describeStream(streamName, limit, exclusiveStartShardId);
        }
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
        try (Timer.Context time = getRecordsTimer.time()) {
            return delegate.getRecords(getRecordsRequest);
        }
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        try (Timer.Context time = getShardIteratorTimer.time()) {
            return delegate.getShardIterator(getShardIteratorRequest);
        }
    }

    @Override
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType) {
        try (Timer.Context time = getShardIteratorTimer.time()) {
            return delegate.getShardIterator(streamName, shardId, shardIteratorType);
        }
    }

    @Override
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType, String startingSequenceNumber) {
        try (Timer.Context time = getShardIteratorTimer.time()) {
            return delegate.getShardIterator(streamName, shardId, shardIteratorType, startingSequenceNumber);
        }
    }

    @Override
    public void increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
        try (Timer.Context time = increaseStreamRetentionPeriodTimer.time()) {
            delegate.increaseStreamRetentionPeriod(increaseStreamRetentionPeriodRequest);
        }
    }

    @Override
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
        try (Timer.Context time = listStreamsTimer.time()) {
            return delegate.listStreams(listStreamsRequest);
        }
    }

    @Override
    public ListStreamsResult listStreams() {
        try (Timer.Context time = listStreamsTimer.time()) {
            return delegate.listStreams();
        }
    }

    @Override
    public ListStreamsResult listStreams(String exclusiveStartStreamName) {
        try (Timer.Context time = listStreamsTimer.time()) {
            return delegate.listStreams(exclusiveStartStreamName);
        }
    }

    @Override
    public ListStreamsResult listStreams(Integer limit, String exclusiveStartStreamName) {
        try (Timer.Context time = listStreamsTimer.time()) {
            return delegate.listStreams(limit, exclusiveStartStreamName);
        }
    }

    @Override
    public ListTagsForStreamResult listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest) {
        try (Timer.Context time = listTagsForStreamTimer.time()) {
            return delegate.listTagsForStream(listTagsForStreamRequest);
        }
    }

    @Override
    public void mergeShards(MergeShardsRequest mergeShardsRequest) {
        try (Timer.Context time = mergeShardsTimer.time()) {
            delegate.mergeShards(mergeShardsRequest);
        }
    }

    @Override
    public void mergeShards(String streamName, String shardToMerge, String adjacentShardToMerge) {
        try (Timer.Context time = mergeShardsTimer.time()) {
            delegate.mergeShards(streamName, shardToMerge, adjacentShardToMerge);
        }
    }

    @Override
    public PutRecordResult putRecord(PutRecordRequest putRecordRequest) {
        try (Timer.Context timer = putRecordTimer.time()) {
            return delegate.putRecord(putRecordRequest);
        }
    }

    @Override
    public PutRecordResult putRecord(String streamName, ByteBuffer data, String partitionKey) {
        try (Timer.Context timer = putRecordTimer.time()) {
            return delegate.putRecord(streamName, data, partitionKey);
        }
    }

    @Override
    public PutRecordResult putRecord(String streamName, ByteBuffer data, String partitionKey, String sequenceNumberForOrdering) {
        try (Timer.Context timer = putRecordTimer.time()) {
            return delegate.putRecord(streamName, data, partitionKey, sequenceNumberForOrdering);
        }
    }

    @Override
    public PutRecordsResult putRecords(PutRecordsRequest putRecordsRequest) {
        try (Timer.Context timer = putRecordsTimer.time()) {
            return delegate.putRecords(putRecordsRequest);
        }
    }

    @Override
    public void removeTagsFromStream(RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
        try (Timer.Context time = removeTagsFromStreamTimer.time()) {
            delegate.removeTagsFromStream(removeTagsFromStreamRequest);
        }
    }

    @Override
    public void splitShard(SplitShardRequest splitShardRequest) {
        try (Timer.Context time = splitShardTimer.time()) {
            delegate.splitShard(splitShardRequest);
        }
    }

    @Override
    public void splitShard(String streamName, String shardToSplit, String newStartingHashKey) {
        try (Timer.Context time = splitShardTimer.time()) {
            delegate.splitShard(streamName, shardToSplit, newStartingHashKey);
        }
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        return delegate.getCachedResponseMetadata(request);
    }
}
