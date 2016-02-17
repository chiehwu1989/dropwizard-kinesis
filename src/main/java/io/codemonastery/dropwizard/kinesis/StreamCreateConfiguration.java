package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.dropwizard.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class StreamCreateConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamCreateConfiguration.class);

    @Min(1)
    private int shardCount = 1;

    @Valid
    @NotNull
    private Duration retryPeriod = Duration.seconds(5);


    @Min(1)
    private Integer maxAttempts;

    @JsonProperty
    public int getShardCount() {
        return shardCount;
    }

    @JsonProperty
    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    @JsonIgnore
    public StreamCreateConfiguration shardCount(int shardCount){
        this.setShardCount(shardCount);
        return this;
    }

    @JsonProperty
    public Duration getRetryPeriod() {
        return retryPeriod;
    }

    @JsonProperty
    public void setRetryPeriod(Duration retryPeriod) {
        this.retryPeriod = retryPeriod;
    }

    @JsonProperty
    public StreamCreateConfiguration retryPeriod(Duration retryPeriod) {
        this.setRetryPeriod(retryPeriod);
        return this;
    }

    @JsonProperty
    public Integer getMaxAttempts() {
        return maxAttempts;
    }

    @JsonProperty
    public void setMaxAttempts(Integer maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    @JsonIgnore
    public StreamCreateConfiguration maxAttempts(Integer maxAttempts) {
        this.setMaxAttempts(maxAttempts);
        return this;
    }

    @JsonIgnore
    public boolean setupStream(AmazonKinesis kinesis, String streamName){
        boolean setup = false;
        Preconditions.checkState(!Strings.isNullOrEmpty(streamName), "streamName was not specified");
        try{
            DescribeStreamResult result;
            if(getRetryPeriod() != null){
                Integer retryAttempts = getMaxAttempts();
                while(retryAttempts == null || retryAttempts > 0){
                    try{
                        result = kinesis.describeStream(streamName);
                        if("active".equalsIgnoreCase(result.getStreamDescription().getStreamStatus())){
                            LOG.info("stream {} is active", streamName);
                            setup = true;
                            break;
                        }
                    }catch (NullPointerException|ResourceNotFoundException e){
                        createStream(kinesis, streamName);
                    }
                    Thread.sleep(retryPeriod.toMilliseconds());
                    if(retryAttempts != null){
                        retryAttempts--;
                    }
                }
            }
        }catch (InterruptedException e){
            LOG.error("Needed to create stream {} but was interrupted, nothing is guaranteed now", streamName);
        }
        return setup;
    }

    private void createStream(AmazonKinesis kinesis, String streamName) {
        LOG.info(String.format("stream %s was not found, creating with %d shards", streamName, getShardCount()));
        try{
            kinesis.createStream(streamName, getShardCount());
        } catch (ResourceInUseException ue){
            LOG.info(String.format("failed to create stream %s because it already existed", streamName));
        } catch (Exception e){
            LOG.error(String.format("failed to create stream %s", e), e);
        }
    }
}
