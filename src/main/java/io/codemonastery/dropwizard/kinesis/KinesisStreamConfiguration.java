package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.dropwizard.util.Duration;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

class KinesisStreamConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamConfiguration.class);

    @NotEmpty
    private String streamName;

    @Min(1)
    private Integer defaultShardCount;

    @Valid
    @NotNull
    private Duration createStreamRetryPeriod = Duration.seconds(5);

    @JsonProperty
    public String getStreamName() {
        return streamName;
    }

    @JsonProperty
    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    @JsonProperty
    public Integer getDefaultShardCount() {
        return defaultShardCount;
    }

    @JsonProperty
    public void setDefaultShardCount(Integer defaultShardCount) {
        this.defaultShardCount = defaultShardCount;
    }

    @JsonProperty
    public Duration getCreateStreamRetryPeriod() {
        return createStreamRetryPeriod;
    }

    @JsonProperty
    public void setCreateStreamRetryPeriod(Duration createStreamRetryPeriod) {
        this.createStreamRetryPeriod = createStreamRetryPeriod;
    }

    void setupStream(AmazonKinesis kinesis){
        Preconditions.checkState(!Strings.isNullOrEmpty(streamName), "streamName was not specified");
        try{
            DescribeStreamResult result;
            if(getDefaultShardCount() != null && createStreamRetryPeriod != null){
                while(true){
                    try{
                        result = kinesis.describeStream(streamName);
                        if("active".equalsIgnoreCase(result.getStreamDescription().getStreamStatus())){
                            LOG.info("stream %s is active", streamName);
                            break;
                        }
                    }catch (NullPointerException|ResourceNotFoundException e){
                        createStream(kinesis);
                    }
                    Thread.sleep(createStreamRetryPeriod.toMilliseconds());
                }
            }
        }catch (InterruptedException e){
            LOG.error("Needed to create stream %s but was interrupted, nothing is guaranteed now", streamName);
        }
    }

    private void createStream(AmazonKinesis kinesis) {
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
