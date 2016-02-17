package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;

import java.util.List;

public class ProducerHealthCheck extends HealthCheck {

    private final ProducerMetrics metrics;
    private final AmazonKinesis kinesis;
    private final String streamName;

    public ProducerHealthCheck(ProducerMetrics metrics, AmazonKinesis kinesis, String streamName) {
        this.metrics = metrics;
        this.kinesis = kinesis;
        this.streamName = streamName;
    }

    @Override
    protected Result check() throws Exception {
        Result result = checkStreamExists();

        //only if we can connect to stream should be check metrics
        if(result.isHealthy()){
            List<String> failed = metrics.highFailureMetrics();
            if(failed != null && failed.size() > 0){
                result = Result.unhealthy(Joiner.on(", ").join(failed));
            }
        }

        return result;
    }

    private Result checkStreamExists() {
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
