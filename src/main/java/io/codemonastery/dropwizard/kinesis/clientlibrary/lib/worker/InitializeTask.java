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
package io.codemonastery.dropwizard.kinesis.clientlibrary.lib.worker;

import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.ICheckpoint;
import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import io.codemonastery.dropwizard.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import io.codemonastery.dropwizard.kinesis.clientlibrary.types.InitializationInput;
import io.codemonastery.dropwizard.kinesis.metrics.impl.MetricsHelper;
import io.codemonastery.dropwizard.kinesis.metrics.interfaces.MetricsLevel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Task for initializing shard position and invoking the RecordProcessor initialize() API.
 */
class InitializeTask implements ITask {

    private static final Log LOG = LogFactory.getLog(InitializeTask.class);

    private static final String RECORD_PROCESSOR_INITIALIZE_METRIC = "RecordProcessor.initialize";

    private final ShardInfo shardInfo;
    private final IRecordProcessor recordProcessor;
    private final KinesisDataFetcher dataFetcher;
    private final TaskType taskType = TaskType.INITIALIZE;
    private final ICheckpoint checkpoint;
    private final RecordProcessorCheckpointer recordProcessorCheckpointer;
    // Back off for this interval if we encounter a problem (exception)
    private final long backoffTimeMillis;

    /**
     * Constructor.
     */
    InitializeTask(ShardInfo shardInfo,
            IRecordProcessor recordProcessor,
            ICheckpoint checkpoint,
            RecordProcessorCheckpointer recordProcessorCheckpointer,
            KinesisDataFetcher dataFetcher,
            long backoffTimeMillis) {
        this.shardInfo = shardInfo;
        this.recordProcessor = recordProcessor;
        this.checkpoint = checkpoint;
        this.recordProcessorCheckpointer = recordProcessorCheckpointer;
        this.dataFetcher = dataFetcher;
        this.backoffTimeMillis = backoffTimeMillis;
    }

    /*
     * Initializes the data fetcher (position in shard) and invokes the RecordProcessor initialize() API.
     * (non-Javadoc)
     *
     * @see ITask#call()
     */
    @Override
    public TaskResult call() {
        boolean applicationException = false;
        Exception exception = null;

        try {
            LOG.debug("Initializing ShardId " + shardInfo.getShardId());
            ExtendedSequenceNumber initialCheckpoint = checkpoint.getCheckpoint(shardInfo.getShardId());

            dataFetcher.initialize(initialCheckpoint.getSequenceNumber());
            recordProcessorCheckpointer.setLargestPermittedCheckpointValue(initialCheckpoint);
            recordProcessorCheckpointer.setInitialCheckpointValue(initialCheckpoint);

            LOG.debug("Calling the record processor initialize().");
            final InitializationInput initializationInput = new InitializationInput()
                .withShardId(shardInfo.getShardId())
                .withExtendedSequenceNumber(initialCheckpoint);
            final long recordProcessorStartTimeMillis = System.currentTimeMillis();
            try {
                recordProcessor.initialize(initializationInput);
                LOG.debug("Record processor initialize() completed.");
            } catch (Exception e) {
                applicationException = true;
                throw e;
            } finally {
                MetricsHelper.addLatency(RECORD_PROCESSOR_INITIALIZE_METRIC, recordProcessorStartTimeMillis,
                        MetricsLevel.SUMMARY);
            }

            return new TaskResult(null);
        } catch (Exception e) {
            if (applicationException) {
                LOG.error("Application initialize() threw exception: ", e);
            } else {
                LOG.error("Caught exception: ", e);
            }
            exception = e;
            // backoff if we encounter an exception.
            try {
                Thread.sleep(this.backoffTimeMillis);
            } catch (InterruptedException ie) {
                LOG.debug("Interrupted sleep", ie);
            }
        }

        return new TaskResult(exception);
    }

    /*
     * (non-Javadoc)
     *
     * @see ITask#getTaskType()
     */
    @Override
    public TaskType getTaskType() {
        return taskType;
    }

}
