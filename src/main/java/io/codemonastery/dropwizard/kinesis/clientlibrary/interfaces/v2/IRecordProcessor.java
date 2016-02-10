/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2;

import io.codemonastery.dropwizard.kinesis.clientlibrary.types.InitializationInput;
import io.codemonastery.dropwizard.kinesis.clientlibrary.types.ProcessRecordsInput;
import io.codemonastery.dropwizard.kinesis.clientlibrary.types.ShutdownInput;

/**
 * The Amazon Kinesis Client Library will instantiate record processors to process data records fetched from Amazon
 * Kinesis.
 */
public interface IRecordProcessor {

    /**
     * Invoked by the Amazon Kinesis Client Library before data records are delivered to the RecordProcessor instance
     * (via processRecords).
     *
     * @param initializationInput Provides information related to initialization 
     */
    void initialize(InitializationInput initializationInput);

    /**
     * Process data records. The Amazon Kinesis Client Library will invoke this method to deliver data records to the
     * application.
     * Upon fail over, the new instance will get records with sequence number greater than checkpoint position
     * for each partition key.
     *
     * @param processRecordsInput Provides the records to be processed as well as information and capabilities related
     *        to them (eg checkpointing).
     */
    void processRecords(ProcessRecordsInput processRecordsInput);

    /**
     * Invoked by the Amazon Kinesis Client Library to indicate it will no longer send data records to this
     * RecordProcessor instance. 
     *
     * @param shutdownInput Provides information and capabilities (eg checkpointing) related to shutdown of this record
     *        processor.
     */
    void shutdown(ShutdownInput shutdownInput);

}
