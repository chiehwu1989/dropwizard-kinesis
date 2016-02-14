package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Min;

public class KinesisStreamConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamConfiguration.class);

    @NotEmpty
    private String streamName;

    @Min(1)
    private int defaultShardCount = 1;

    @JsonProperty
    public String getStreamName() {
        return streamName;
    }

    @JsonProperty
    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    @JsonProperty
    public int getDefaultShardCount() {
        return defaultShardCount;
    }

    @JsonProperty
    public void setDefaultShardCount(int defaultShardCount) {
        this.defaultShardCount = defaultShardCount;
    }

    public void setupStream(AmazonKinesis kinesis){
        Preconditions.checkState(!Strings.isNullOrEmpty(streamName), "streamName was not specified");
        try{
            kinesis.describeStream(streamName);
        }catch (ResourceNotFoundException re){
            LOG.info(String.format("stream %s was not found, creating with %d shards", streamName, defaultShardCount));
            try{
                kinesis.createStream(streamName, defaultShardCount);
            } catch (ResourceInUseException ue){
                LOG.info(String.format("failed to create stream %s because it already existed", streamName));
            } catch (Exception e){
                LOG.error(String.format("failed to create stream %s", e), e);
            }
        }
    }

}
