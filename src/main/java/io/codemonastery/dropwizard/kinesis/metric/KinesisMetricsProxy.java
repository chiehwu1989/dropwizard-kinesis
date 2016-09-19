package io.codemonastery.dropwizard.kinesis.metric;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.kinesis.waiters.AmazonKinesisWaiters;
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
        getRecordsTimer = metrics.timer(name + "-get-records");
        getShardIteratorTimer = metrics.timer(name + "-get-shard-iterator");
        increaseStreamRetentionPeriodTimer = metrics.timer(name + "-increase-stream-retention-period");
        listStreamsTimer = metrics.timer(name + "-list-streams");
        listTagsForStreamTimer = metrics.timer(name + "-list-tags-for-stream");
        mergeShardsTimer = metrics.timer(name + "-merge-shards");
        putRecordTimer = metrics.timer(name + "-put-record");
        putRecordsTimer = metrics.timer(name + "-put-records");
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
    public AddTagsToStreamResult addTagsToStream(AddTagsToStreamRequest addTagsToStreamRequest) {
        final AddTagsToStreamResult addTagsToStreamResult;
        try (Timer.Context time = addTagsToStreamTimer.time()) {
            addTagsToStreamResult = delegate.addTagsToStream(addTagsToStreamRequest);
        }
        return addTagsToStreamResult;
    }

    @Override
    public CreateStreamResult createStream(CreateStreamRequest createStreamRequest) {
        final CreateStreamResult createStreamResult;
        try (Timer.Context time = createStreamTimer.time()) {
            createStreamResult = delegate.createStream(createStreamRequest);
        }
        return createStreamResult;
    }

    @Override
    public CreateStreamResult createStream(String streamName, Integer shardCount) {
        final CreateStreamResult createStreamResult;
        try (Timer.Context time = createStreamTimer.time()) {
            createStreamResult = delegate.createStream(streamName, shardCount);
        }
        return createStreamResult;
    }

    @Override
    public DecreaseStreamRetentionPeriodResult decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
        final DecreaseStreamRetentionPeriodResult decreaseStreamRetentionPeriodResult;
        try (Timer.Context time = decreaseStreamRetentionPeriodTimer.time()) {
            decreaseStreamRetentionPeriodResult = delegate.decreaseStreamRetentionPeriod(decreaseStreamRetentionPeriodRequest);
        }
        return decreaseStreamRetentionPeriodResult;
    }

    @Override
    public DeleteStreamResult deleteStream(DeleteStreamRequest deleteStreamRequest) {
        final DeleteStreamResult deleteStreamResult;
        try (Timer.Context time = deleteStreamTimer.time()) {
            deleteStreamResult = delegate.deleteStream(deleteStreamRequest);
        }
        return deleteStreamResult;
    }

    @Override
    public DeleteStreamResult deleteStream(String streamName) {
        final DeleteStreamResult deleteStreamResult;
        try (Timer.Context time = deleteStreamTimer.time()) {
            deleteStreamResult = delegate.deleteStream(streamName);
        }
        return deleteStreamResult;
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
    public DisableEnhancedMonitoringResult disableEnhancedMonitoring(DisableEnhancedMonitoringRequest disableEnhancedMonitoringRequest) {
        return null;
    }

    @Override
    public EnableEnhancedMonitoringResult enableEnhancedMonitoring(EnableEnhancedMonitoringRequest enableEnhancedMonitoringRequest) {
        return null;
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
    public IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
        final IncreaseStreamRetentionPeriodResult increaseStreamRetentionPeriodResult;
        try (Timer.Context time = increaseStreamRetentionPeriodTimer.time()) {
            increaseStreamRetentionPeriodResult = delegate.increaseStreamRetentionPeriod(increaseStreamRetentionPeriodRequest);
        }
        return increaseStreamRetentionPeriodResult;
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
    public MergeShardsResult mergeShards(MergeShardsRequest mergeShardsRequest) {
        final MergeShardsResult mergeShardsResult;
        try (Timer.Context time = mergeShardsTimer.time()) {
            mergeShardsResult = delegate.mergeShards(mergeShardsRequest);
        }
        return mergeShardsResult;
    }

    @Override
    public MergeShardsResult mergeShards(String streamName, String shardToMerge, String adjacentShardToMerge) {
        final MergeShardsResult mergeShardsResult;
        try (Timer.Context time = mergeShardsTimer.time()) {
            mergeShardsResult = delegate.mergeShards(streamName, shardToMerge, adjacentShardToMerge);
        }
        return mergeShardsResult;
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
    public RemoveTagsFromStreamResult removeTagsFromStream(RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
        final RemoveTagsFromStreamResult removeTagsFromStreamResult;
        try (Timer.Context time = removeTagsFromStreamTimer.time()) {
            removeTagsFromStreamResult = delegate.removeTagsFromStream(removeTagsFromStreamRequest);
        }
        return removeTagsFromStreamResult;
    }

    @Override
    public SplitShardResult splitShard(SplitShardRequest splitShardRequest) {
        final SplitShardResult splitShardResult;
        try (Timer.Context time = splitShardTimer.time()) {
            splitShardResult = delegate.splitShard(splitShardRequest);
        }
        return splitShardResult;
    }

    @Override
    public SplitShardResult splitShard(String streamName, String shardToSplit, String newStartingHashKey) {
        final SplitShardResult splitShardResult;
        try (Timer.Context time = splitShardTimer.time()) {
            splitShardResult = delegate.splitShard(streamName, shardToSplit, newStartingHashKey);
        }
        return splitShardResult;
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        return delegate.getCachedResponseMetadata(request);
    }

    @Override
    public AmazonKinesisWaiters waiters() {
        return new AmazonKinesisWaiters(delegate);
    }
}
