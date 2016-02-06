/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Utilities to create and delete Amazon Kinesis streams.
 */
public class KinesisClientUtil {

    private static Log LOG = LogFactory.getLog(KinesisClientUtil.class);


    public static AmazonKinesis createKinesisClient(Region region, String endpoint, String userAgent, String awsProfileName) {
        final CredentialUtils awsCredentialsProvider = new CredentialUtils(awsProfileName);
        return createKinesisClient(region, endpoint, awsCredentialsProvider, userAgent);
    }

    public static AmazonKinesis createKinesisClient(Region region, String endpoint, String userAgent) {
        final CredentialUtils awsCredentialsProvider = new CredentialUtils();
        return createKinesisClient(region, endpoint, awsCredentialsProvider, userAgent);
    }

    public static AmazonKinesis createKinesisClient(Region region, String endpoint) {
        final CredentialUtils awsCredentialsProvider = new CredentialUtils();
        final String userAgent = ClientConfiguration.DEFAULT_USER_AGENT;
        return createKinesisClient(region, endpoint, awsCredentialsProvider, userAgent);
    }

    private static AmazonKinesis createKinesisClient(Region region,
                                                     String endpoint,
                                                     CredentialUtils awsCredentialsProvider,
                                                     String userAgent) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withUserAgent(ClientConfiguration.DEFAULT_USER_AGENT + "-" + userAgent);
        AmazonKinesisClient kinesisClient = new AmazonKinesisClient(
                awsCredentialsProvider,
                clientConfiguration);
        kinesisClient.setRegion(region);
        kinesisClient.setEndpoint(endpoint);

        return kinesisClient;
    }

    /**
     * Creates an Amazon Kinesis stream if it does not exist and waits for it to become available
     *
     * @param kinesisClient The {@link AmazonKinesis} with Amazon Kinesis read and write privileges
     * @param streamName    The Amazon Kinesis stream name to create
     * @param shardCount    The shard count to create the stream with
     * @throws IllegalStateException Invalid Amazon Kinesis stream state
     * @throws IllegalStateException Stream does not go active before the timeout
     */
    public static void createAndWaitForStreamToBecomeAvailable(AmazonKinesis kinesisClient,
                                                               String streamName,
                                                               int shardCount) {
        if (streamExists(kinesisClient, streamName)) {
            String state = streamState(kinesisClient, streamName);
            if (logValidStreamState(kinesisClient, streamName, state)) return;
        } else {
            CreateStreamRequest createStreamRequest = new CreateStreamRequest();
            createStreamRequest.setStreamName(streamName);
            createStreamRequest.setShardCount(shardCount);
            kinesisClient.createStream(createStreamRequest);
            LOG.info("Stream " + streamName + " created");
        }
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(1000 * 10);
            } catch (Exception e) {
            }
            try {
                String streamStatus = streamState(kinesisClient, streamName);
                if (streamStatus.equals("ACTIVE")) {
                    LOG.info("Stream " + streamName + " is ACTIVE");
                    return;
                }
            } catch (ResourceNotFoundException e) {
                throw new IllegalStateException("Stream " + streamName + " never went active");
            }
        }
    }

    private static boolean logValidStreamState(AmazonKinesis kinesisClient, String streamName, String state) {
        if (state.equals("DELETING")) {
            long startTime = System.currentTimeMillis();
            long endTime = startTime + 1000 * 120;
            while (System.currentTimeMillis() < endTime && streamExists(kinesisClient, streamName)) {
                try {
                    LOG.info("...Deleting Stream " + streamName + "...");
                    Thread.sleep(1000 * 10);
                } catch (InterruptedException e) {
                }
            }
            if (streamExists(kinesisClient, streamName)) {
                LOG.error("ClientUtil timed out waiting for stream " + streamName + " to delete");
                throw new IllegalStateException("KinesisClientUtil timed out waiting for stream " + streamName
                        + " to delete");
            }

            LOG.info("Stream " + streamName + " is ACTIVE");
            return true;
        } else if (state.equals("ACTIVE")) {
            LOG.info("Stream " + streamName + " is ACTIVE");
            return true;
        } else if (state.equals("CREATING")) {
        } else if (state.equals("UPDATING")) {
            LOG.info("Stream " + streamName + " is UPDATING");
            return true;
        } else {
            throw new IllegalStateException("Illegal stream state: " + state);
        }
        return false;
    }

    /**
     * Checks if the stream exists and is active
     *
     * @param kinesisClient Amazon Kinesis client instance
     * @param streamName    Name of stream
     * @return true if the Amazon Kinesis stream is Active, otherwise return false
     */
    public static boolean validateStream(AmazonKinesis kinesisClient, String streamName) {
        try {
            DescribeStreamResult result = kinesisClient.describeStream(streamName);
            if (!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                LOG.warn("Stream " + streamName + " is not active. Please wait a few moments and try again.");
            }
            return true;
        } catch (ResourceNotFoundException e) {
            LOG.error("Stream " + streamName + " does not exist. Please create it in the console.", e);
            return false;
        } catch (Exception e) {
            LOG.error("Error found while describing the stream " + streamName, e);
            return false;
        }
    }

    /**
     * Helper method to determine if an Amazon Kinesis stream exists.
     *
     * @param kinesisClient The {@link AmazonKinesis} with Amazon Kinesis read privileges
     * @param streamName    The Amazon Kinesis stream to check for
     * @return true if the Amazon Kinesis stream exists, otherwise return false
     */
    private static boolean streamExists(AmazonKinesis kinesisClient, String streamName) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        try {
            kinesisClient.describeStream(describeStreamRequest);
            return true;
        } catch (ResourceNotFoundException e) {
            return false;
        }
    }

    /**
     * Return the state of a Amazon Kinesis stream.
     *
     * @param kinesisClient The {@link AmazonKinesis} with Amazon Kinesis read privileges
     * @param streamName    The Amazon Kinesis stream to get the state of
     * @return String representation of the Stream state
     */
    private static String streamState(AmazonKinesis kinesisClient, String streamName) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        try {
            return kinesisClient.describeStream(describeStreamRequest).getStreamDescription().getStreamStatus();
        } catch (AmazonServiceException e) {
            return null;
        }
    }

    /**
     * Gets a list of all Amazon Kinesis streams
     *
     * @param kinesisClient The {@link AmazonKinesis} with Amazon Kinesis read privileges
     * @return list of Amazon Kinesis streams
     */
    public static List<String> listAllStreams(AmazonKinesis kinesisClient) {

        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(10);
        ListStreamsResult listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();
        while (listStreamsResult.isHasMoreStreams()) {
            if (streamNames.size() > 0) {
                listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
            }

            listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
            streamNames.addAll(listStreamsResult.getStreamNames());
        }
        return streamNames;
    }

    /**
     * Deletes an Amazon Kinesis stream if it exists.
     *
     * @param kinesisClient The {@link AmazonKinesis} with Amazon Kinesis read and write privileges
     * @param streamName    The Amazon Kinesis stream to delete
     */
    public static void deleteStream(AmazonKinesis kinesisClient, String streamName) {
        if (streamExists(kinesisClient, streamName)) {
            DeleteStreamRequest deleteStreamRequest = new DeleteStreamRequest();
            deleteStreamRequest.setStreamName(streamName);
            kinesisClient.deleteStream(deleteStreamRequest);
            LOG.info("Deleting stream " + streamName);
        } else {
            LOG.warn("Stream " + streamName + " does not exist");
        }
    }
}
