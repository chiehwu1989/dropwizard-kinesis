/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.codemonastery.dropwizard.kinesis.clientlibrary.proxies;

import io.codemonastery.dropwizard.kinesis.metrics.impl.MetricsHelper;
import io.codemonastery.dropwizard.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * IKinesisProxy implementation that wraps another implementation and collects metrics.
 */
public class MetricsCollectingKinesisProxyDecorator implements IKinesisProxy {

    private static final String SEP = ".";

    private final String getIteratorMetric;
    private final String getRecordsMetric;
    private final String getStreamInfoMetric;
    private final String getShardListMetric;
    private final String putRecordMetric;
    private final String getRecordsShardId;

    private IKinesisProxy other;

    /**
     * Constructor.
     * 
     * @param prefix prefix for generated metrics
     * @param other Kinesis proxy to decorate
     * @param shardId shardId will be included in the metrics.
     */
    public MetricsCollectingKinesisProxyDecorator(String prefix, IKinesisProxy other, String shardId) {
        this.other = other;
        getRecordsShardId = shardId;
        getIteratorMetric = prefix + SEP + "getIterator";
        getRecordsMetric = prefix + SEP + "getRecords";
        getStreamInfoMetric = prefix + SEP + "getStreamInfo";
        getShardListMetric = prefix + SEP + "getShardList";
        putRecordMetric = prefix + SEP + "putRecord";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GetRecordsResult get(String shardIterator, int maxRecords)
        throws ResourceNotFoundException, InvalidArgumentException, ExpiredIteratorException {
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            GetRecordsResult response = other.get(shardIterator, maxRecords);
            success = true;
            return response;
        } finally {
            MetricsHelper.addSuccessAndLatencyPerShard(getRecordsShardId, getRecordsMetric, startTime, success,
                    MetricsLevel.DETAILED);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DescribeStreamResult getStreamInfo(String startingShardId) throws ResourceNotFoundException {
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            DescribeStreamResult response = other.getStreamInfo(startingShardId);
            success = true;
            return response;
        } finally {
            MetricsHelper.addSuccessAndLatency(getStreamInfoMetric, startTime, success, MetricsLevel.DETAILED);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllShardIds() throws ResourceNotFoundException {
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            Set<String> response = other.getAllShardIds();
            success = true;
            return response;
        } finally {
            MetricsHelper.addSuccessAndLatency(getStreamInfoMetric, startTime, success, MetricsLevel.DETAILED);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIterator(String shardId, String iteratorEnum, String sequenceNumber)
        throws ResourceNotFoundException, InvalidArgumentException {
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            String response = other.getIterator(shardId, iteratorEnum, sequenceNumber);
            success = true;
            return response;
        } finally {
            MetricsHelper.addSuccessAndLatency(getIteratorMetric, startTime, success, MetricsLevel.DETAILED);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Shard> getShardList() throws ResourceNotFoundException {
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            List<Shard> response = other.getShardList();
            success = true;
            return response;
        } finally {
            MetricsHelper.addSuccessAndLatency(getShardListMetric, startTime, success, MetricsLevel.DETAILED);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PutRecordResult put(String sequenceNumberForOrdering,
                               String explicitHashKey,
                               String partitionKey,
                               ByteBuffer data) throws ResourceNotFoundException, InvalidArgumentException {
        long startTime = System.currentTimeMillis();
        boolean success = false;
        try {
            PutRecordResult response = other.put(sequenceNumberForOrdering, explicitHashKey, partitionKey, data);
            success = true;
            return response;
        } finally {
            MetricsHelper.addSuccessAndLatency(putRecordMetric, startTime, success, MetricsLevel.DETAILED);
        }
    }
}
