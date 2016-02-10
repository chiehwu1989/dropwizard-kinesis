package io.codemonastery.dropwizard.kinesis.example;/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import io.codemonastery.dropwizard.kinesis.CredentialUtils;
import io.codemonastery.dropwizard.kinesis.KinesisClientUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class ProducerExample {
private static final Logger LOG = LoggerFactory.getLogger(ProducerExample.class);

    public static void main(String[] args) throws Exception {

        final AWSCredentialsProviderChain credentialsProvider = new CredentialUtils();
        AmazonKinesisClient kinesis = new AmazonKinesisClient(credentialsProvider.getCredentials());

        final String myStreamName = ConsumerExample.SAMPLE_APPLICATION_STREAM_NAME;
        KinesisClientUtil.createAndWaitForStreamToBecomeAvailable(kinesis, myStreamName, 1);


        LOG.info(String.format("Putting records in stream : " +
                "%s until this application is stopped...\nPress CTRL-C to stop.", myStreamName));
        // Write records to the stream until this program is aborted.
        while (true) {
            long createTime = System.currentTimeMillis();
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(myStreamName);
            putRecordRequest.setData(ByteBuffer.wrap(String.format("testData-%d", createTime).getBytes()));
            putRecordRequest.setPartitionKey(String.format("partitionKey-%d", createTime));
            PutRecordResult putRecordResult = kinesis.putRecord(putRecordRequest);
            LOG.info(String.format("Successfully put record, partition key : %s, ShardID : %s, SequenceNumber : %s.\n",
                    putRecordRequest.getPartitionKey(),
                    putRecordResult.getShardId(),
                    putRecordResult.getSequenceNumber()));
        }
    }

}
