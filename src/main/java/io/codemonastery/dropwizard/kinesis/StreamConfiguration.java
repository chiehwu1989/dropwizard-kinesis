package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;


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
        Preconditions.checkNotNull(streamName, "stream name cannot be null");
        boolean setup = true;
        if (create != null) {
            setup = create.setupStream(kinesis, streamName);
        }
        return setup;
    }
}
