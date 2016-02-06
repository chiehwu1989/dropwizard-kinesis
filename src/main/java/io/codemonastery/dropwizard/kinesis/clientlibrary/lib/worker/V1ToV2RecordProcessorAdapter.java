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
package io.codemonastery.dropwizard.kinesis.clientlibrary.lib.worker;

import io.codemonastery.dropwizard.kinesis.clientlibrary.types.InitializationInput;
import io.codemonastery.dropwizard.kinesis.clientlibrary.types.ProcessRecordsInput;
import io.codemonastery.dropwizard.kinesis.clientlibrary.types.ShutdownInput;

/**
 * Adapts a V1 {@link io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.IRecordProcessor IRecordProcessor}
 * to V2 {@link io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessor IRecordProcessor}.
 */
class V1ToV2RecordProcessorAdapter implements io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessor {

    private io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.IRecordProcessor recordProcessor;
    
    V1ToV2RecordProcessorAdapter(
            io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.IRecordProcessor recordProcessor) {
        this.recordProcessor = recordProcessor;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        recordProcessor.initialize(initializationInput.getShardId());  
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        recordProcessor.processRecords(processRecordsInput.getRecords(), processRecordsInput.getCheckpointer());
        
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        recordProcessor.shutdown(shutdownInput.getCheckpointer(), shutdownInput.getShutdownReason());
    }

}
