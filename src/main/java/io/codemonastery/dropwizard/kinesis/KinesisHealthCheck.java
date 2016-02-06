package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.codahale.metrics.health.HealthCheck;


public class KinesisHealthCheck extends HealthCheck {

    private final AmazonKinesis kinesisClient;
    private final String streamName;

    public KinesisHealthCheck(AmazonKinesis kinesisClient, String streamName) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
    }

    @Override
    protected Result check() throws Exception {
        try {
            DescribeStreamResult result = kinesisClient.describeStream(streamName);
            if (!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
                return Result.unhealthy("Stream " + streamName + " is not active. Please wait a few moments and try again.");
            }
        } catch (ResourceNotFoundException e) {
            return Result.unhealthy("Stream " + streamName + " does not exist.");
        } catch (Exception e) {
            return Result.unhealthy("Error found while describing the stream " + streamName);
        }
        return Result.healthy();
    }
}