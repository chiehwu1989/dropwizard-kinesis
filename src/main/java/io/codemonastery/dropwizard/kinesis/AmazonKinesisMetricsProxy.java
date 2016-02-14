package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

public class AmazonKinesisMetricsProxy implements AmazonKinesis {

    private final AmazonKinesis delegate;

    public AmazonKinesisMetricsProxy(AmazonKinesis delegate) {
        Preconditions.checkNotNull(delegate, "delegate cannot be null");
        this.delegate = delegate;
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
        delegate.addTagsToStream(addTagsToStreamRequest);
    }

    @Override
    public void createStream(CreateStreamRequest createStreamRequest) {
        delegate.createStream(createStreamRequest);
    }

    @Override
    public void createStream(String streamName, Integer shardCount) {
        delegate.createStream(streamName, shardCount);
    }

    @Override
    public void decreaseStreamRetentionPeriod(DecreaseStreamRetentionPeriodRequest decreaseStreamRetentionPeriodRequest) {
        delegate.decreaseStreamRetentionPeriod(decreaseStreamRetentionPeriodRequest);
    }

    @Override
    public void deleteStream(DeleteStreamRequest deleteStreamRequest) {
        delegate.deleteStream(deleteStreamRequest);
    }

    @Override
    public void deleteStream(String streamName) {
        delegate.deleteStream(streamName);
    }

    @Override
    public DescribeStreamResult describeStream(DescribeStreamRequest describeStreamRequest) {
        return delegate.describeStream(describeStreamRequest);
    }

    @Override
    public DescribeStreamResult describeStream(String streamName) {
        return delegate.describeStream(streamName);
    }

    @Override
    public DescribeStreamResult describeStream(String streamName, String exclusiveStartShardId) {
        return delegate.describeStream(streamName, exclusiveStartShardId);
    }

    @Override
    public DescribeStreamResult describeStream(String streamName, Integer limit, String exclusiveStartShardId) {
        return delegate.describeStream(streamName, limit, exclusiveStartShardId);
    }

    @Override
    public GetRecordsResult getRecords(GetRecordsRequest getRecordsRequest) {
        return delegate.getRecords(getRecordsRequest);
    }

    @Override
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest getShardIteratorRequest) {
        return delegate.getShardIterator(getShardIteratorRequest);
    }

    @Override
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType) {
        return delegate.getShardIterator(streamName, shardId, shardIteratorType);
    }

    @Override
    public GetShardIteratorResult getShardIterator(String streamName, String shardId, String shardIteratorType, String startingSequenceNumber) {
        return delegate.getShardIterator(streamName, shardId, shardIteratorType, startingSequenceNumber);
    }

    @Override
    public void increaseStreamRetentionPeriod(IncreaseStreamRetentionPeriodRequest increaseStreamRetentionPeriodRequest) {
        delegate.increaseStreamRetentionPeriod(increaseStreamRetentionPeriodRequest);
    }

    @Override
    public ListStreamsResult listStreams(ListStreamsRequest listStreamsRequest) {
        return delegate.listStreams(listStreamsRequest);
    }

    @Override
    public ListStreamsResult listStreams() {
        return delegate.listStreams();
    }

    @Override
    public ListStreamsResult listStreams(String exclusiveStartStreamName) {
        return delegate.listStreams(exclusiveStartStreamName);
    }

    @Override
    public ListStreamsResult listStreams(Integer limit, String exclusiveStartStreamName) {
        return delegate.listStreams(limit, exclusiveStartStreamName);
    }

    @Override
    public ListTagsForStreamResult listTagsForStream(ListTagsForStreamRequest listTagsForStreamRequest) {
        return delegate.listTagsForStream(listTagsForStreamRequest);
    }

    @Override
    public void mergeShards(MergeShardsRequest mergeShardsRequest) {
        delegate.mergeShards(mergeShardsRequest);
    }

    @Override
    public void mergeShards(String streamName, String shardToMerge, String adjacentShardToMerge) {
        delegate.mergeShards(streamName, shardToMerge, adjacentShardToMerge);
    }

    @Override
    public PutRecordResult putRecord(PutRecordRequest putRecordRequest) {
        return delegate.putRecord(putRecordRequest);
    }

    @Override
    public PutRecordResult putRecord(String streamName, ByteBuffer data, String partitionKey) {
        return delegate.putRecord(streamName, data, partitionKey);
    }

    @Override
    public PutRecordResult putRecord(String streamName, ByteBuffer data, String partitionKey, String sequenceNumberForOrdering) {
        return delegate.putRecord(streamName, data, partitionKey, sequenceNumberForOrdering);
    }

    @Override
    public PutRecordsResult putRecords(PutRecordsRequest putRecordsRequest) {
        return delegate.putRecords(putRecordsRequest);
    }

    @Override
    public void removeTagsFromStream(RemoveTagsFromStreamRequest removeTagsFromStreamRequest) {
        delegate.removeTagsFromStream(removeTagsFromStreamRequest);
    }

    @Override
    public void splitShard(SplitShardRequest splitShardRequest) {
        delegate.splitShard(splitShardRequest);
    }

    @Override
    public void splitShard(String streamName, String shardToSplit, String newStartingHashKey) {
        delegate.splitShard(streamName, shardToSplit, newStartingHashKey);
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
