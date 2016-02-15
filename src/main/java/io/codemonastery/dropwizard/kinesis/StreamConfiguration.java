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
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;


public class StreamConfiguration {

    @NotEmpty
    private String streamName;

    @Valid
    private StreamCreateConfiguration create;

    @JsonProperty
    public String getStreamName() {
        return streamName;
    }

    @JsonProperty
    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    @JsonProperty
    public StreamCreateConfiguration getCreate() {
        return create;
    }

    @JsonProperty
    public void setCreate(StreamCreateConfiguration create) {
        this.create = create;
    }

    @JsonIgnore
    protected boolean setupStream(AmazonKinesis kinesis) {
        boolean setup = true;
        if (create != null) {
            setup = create.setupStream(kinesis, streamName);
        }
        return setup;
    }
}
