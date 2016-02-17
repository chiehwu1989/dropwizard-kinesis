package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class AwsCredentialsFactory implements AWSCredentialsProvider {

    @NotEmpty
    private String accessKey;

    @NotEmpty
    private String secretAccessKey;

    @JsonProperty
    public String getAccessKey() {
        return accessKey;
    }

    @JsonProperty
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    @JsonIgnore
    public AwsCredentialsFactory accessKey(String accessKey) {
        this.setAccessKey(accessKey);
        return this;
    }

    @JsonProperty
    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    @JsonProperty
    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }

    @JsonIgnore
    public AwsCredentialsFactory secretAccessKey(String secretAccessKey) {
        this.setSecretAccessKey(secretAccessKey);
        return this;
    }

    @Override
    public AWSCredentials getCredentials() {
        return new BasicAWSCredentials(accessKey, secretAccessKey);
    }

    @Override
    public void refresh() {
        //no op
    }
}
