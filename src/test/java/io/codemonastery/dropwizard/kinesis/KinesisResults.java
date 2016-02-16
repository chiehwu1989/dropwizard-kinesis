package io.codemonastery.dropwizard.kinesis;


import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.StreamDescription;

public class KinesisResults {

    public static DescribeStreamResult activeStream(String streamName){
        return stream(streamName, "ACTIVE");
    }

    public static DescribeStreamResult creatingStream(String streamName) {
        return stream(streamName, "CREATING");
    }

    public static DescribeStreamResult stream(String streamName, String status) {
        return new DescribeStreamResult()
                .withStreamDescription(
                        new StreamDescription()
                                .withStreamName(streamName)
                                .withStreamStatus(status)
                );
    }

    private KinesisResults() {
    }
}
