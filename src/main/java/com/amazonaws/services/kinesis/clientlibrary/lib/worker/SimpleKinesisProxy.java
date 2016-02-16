package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.proxies.IKinesisProxyExtended;
import com.amazonaws.services.kinesis.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/*
Cleaner implementation of KinesisProxy which was throwing exceptions
 */
public final class SimpleKinesisProxy implements IKinesisProxyExtended {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleKinesisProxy.class);

    private static final long DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS = 1000L;
    private static final int DEFAULT_DESCRIBE_STREAM_RETRY_TIMES = 50;

    private final AmazonKinesis client;
    private final String streamName;
    private final long describeStreamBackoffTimeInMillis;
    private final int maxDescribeStreamRetryAttempts;

    private AtomicReference<List<Shard>> listOfShardsSinceLastGet = new AtomicReference<>();

    public SimpleKinesisProxy(AmazonKinesis client,
                              String streamName){
        this(client, streamName, DEFAULT_DESCRIBE_STREAM_BACKOFF_MILLIS, DEFAULT_DESCRIBE_STREAM_RETRY_TIMES);
    }

    public SimpleKinesisProxy(AmazonKinesis client,
                              String streamName,
                              long describeStreamBackoffTimeInMillis,
                              int maxDescribeStreamRetryAttempts) {
        this.client = client;
        this.streamName = streamName;
        this.describeStreamBackoffTimeInMillis = describeStreamBackoffTimeInMillis;
        this.maxDescribeStreamRetryAttempts = maxDescribeStreamRetryAttempts;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GetRecordsResult get(String shardIterator, int maxRecords)
            throws ResourceNotFoundException, InvalidArgumentException, ExpiredIteratorException {

        final GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setShardIterator(shardIterator);
        getRecordsRequest.setLimit(maxRecords);
        return client.getRecords(getRecordsRequest);

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DescribeStreamResult getStreamInfo(String startShardId)
            throws ResourceNotFoundException, LimitExceededException {
        final DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        describeStreamRequest.setExclusiveStartShardId(startShardId);
        DescribeStreamResult response = null;
        int remainingRetryTimes = this.maxDescribeStreamRetryAttempts;
        // Call DescribeStream, with backoff and retries (if we get LimitExceededException).
        while ((remainingRetryTimes >= 0) && (response == null)) {
            try {
                response = client.describeStream(describeStreamRequest);
            } catch (LimitExceededException le) {
                LOG.info("Got LimitExceededException when describing stream " + streamName + ". Backing off for "
                        + this.describeStreamBackoffTimeInMillis + " millis.");
                try {
                    Thread.sleep(this.describeStreamBackoffTimeInMillis);
                } catch (InterruptedException ie) {
                    LOG.debug("Stream " + streamName + " : Sleep  was interrupted ", ie);
                }
            }
            remainingRetryTimes--;
        }

        if(response != null){
            if (StreamStatus.ACTIVE.toString().equals(response.getStreamDescription().getStreamStatus())
                    || StreamStatus.UPDATING.toString().equals(response.getStreamDescription().getStreamStatus())) {
                return response;
            } else {
                LOG.info("Stream is in status " + response.getStreamDescription().getStreamStatus()
                        + ", KinesisProxy.DescribeStream returning null (wait until stream is Active or Updating");
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Shard getShard(String shardId) {
        if (this.listOfShardsSinceLastGet.get() == null) {
            //Update this.listOfShardsSinceLastGet as needed.
            this.getShardList();
        }

        for (Shard shard : listOfShardsSinceLastGet.get()) {
            if (shard.getShardId().equals(shardId))  {
                return shard;
            }
        }

        LOG.warn("Cannot find the shard given the shardId " + shardId);
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Shard> getShardList() {
        List<Shard> result = new ArrayList<>();

        DescribeStreamResult response;
        String lastShardId = null;

        do {
            response = getStreamInfo(lastShardId);

            if (response == null) {
                /*
                 * If getStreamInfo ever returns null, we should bail and return null. This indicates the stream is not
                 * in ACTIVE or UPDATING state and we may not have accurate/consistent information about the stream.
                 */
                return null;
            } else {
                List<Shard> shards = response.getStreamDescription().getShards();
                result.addAll(shards);
                lastShardId = shards.get(shards.size() - 1).getShardId();
            }
        } while (response.getStreamDescription().isHasMoreShards());
        this.listOfShardsSinceLastGet.set(result);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllShardIds() throws ResourceNotFoundException {
        List<Shard> shards = getShardList();
        if (shards == null) {
            return null;
        } else {
            Set<String> shardIds = new HashSet<>();

            for (Shard shard : getShardList()) {
                shardIds.add(shard.getShardId());
            }

            return shardIds;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIterator(String shardId, String iteratorType, String sequenceNumber) {
        final GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(streamName);
        getShardIteratorRequest.setShardId(shardId);
        getShardIteratorRequest.setShardIteratorType(iteratorType);
        getShardIteratorRequest.setStartingSequenceNumber(sequenceNumber);
        final GetShardIteratorResult response = client.getShardIterator(getShardIteratorRequest);
        return response.getShardIterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PutRecordResult put(String exclusiveMinimumSequenceNumber,
                               String explicitHashKey,
                               String partitionKey,
                               ByteBuffer data) throws ResourceNotFoundException, InvalidArgumentException {
        final PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(streamName);
        putRecordRequest.setSequenceNumberForOrdering(exclusiveMinimumSequenceNumber);
        putRecordRequest.setExplicitHashKey(explicitHashKey);
        putRecordRequest.setPartitionKey(partitionKey);
        putRecordRequest.setData(data);

        return client.putRecord(putRecordRequest);
    }
}
