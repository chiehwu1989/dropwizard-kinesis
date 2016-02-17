package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.codahale.metrics.health.HealthCheck;

public class StreamHealthCheck extends HealthCheck {

    private final AmazonKinesis kinesis;
    private final String streamName;

    public StreamHealthCheck(AmazonKinesis kinesis, String streamName) {
        this.kinesis = kinesis;
        this.streamName = streamName;
    }

    @Override
    protected Result check() throws Exception {
        Result result = Result.healthy();
        try{
            DescribeStreamResult describeResult = kinesis.describeStream(streamName);
            if(describeResult == null || describeResult.getStreamDescription() == null ||
                    !"active".equalsIgnoreCase(describeResult.getStreamDescription().getStreamStatus())){
                result = Result.unhealthy(streamName + " exists but is not active");
            }
        }catch (ResourceNotFoundException e){
            result = Result.unhealthy(streamName + " does not exist");
        }catch (Exception e){
            result = Result.unhealthy(new Exception("could not check status of stream", e));
        }
        return result;
    }
}
