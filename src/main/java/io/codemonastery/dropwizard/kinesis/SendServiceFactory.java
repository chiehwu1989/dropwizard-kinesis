package io.codemonastery.dropwizard.kinesis;


import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.setup.Environment;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class SendServiceFactory {

    @Min(1024)
    @Max(5 * 1024 * 1024)
    protected int bufferSize = 1024 * 1024 * 5;

    @Min(1)
    @Max(500)
    protected int maxElements = 500;


    @Min(1)
    protected int deliveryThreads = 10;

    @Min(1)
    protected int flushPeriodSeconds = 60;

    protected Region region = Region.getRegion(Regions.US_WEST_2);

    @NotNull
    protected String streamName;

    @NotNull
    protected String endpoint;

    protected String bucketARN;

    protected String roleARN;

    @JsonProperty
    public String getBucketARN() {
        return bucketARN;
    }

    @JsonProperty
    public void setBucketARN(String bucketARN) {
        this.bucketARN = bucketARN;
    }

    @JsonProperty
    public String getRoleARN() {
        return roleARN;
    }

    @JsonProperty
    public void setRoleARN(String roleARN) {
        this.roleARN = roleARN;
    }

    @JsonProperty
    public int getMaxElements() {
        return maxElements;
    }

    @JsonProperty
    public void setMaxElements(int maxElements) {
        this.maxElements = maxElements;
    }

    @JsonProperty
    public Region getRegion() {
        return region;
    }


    @JsonProperty
    public void setRegion(String region) {
        this.region = Region.getRegion(Regions.fromName(region));
    }

    @JsonProperty
    public String getStreamName() {
        return streamName;
    }

    @JsonProperty
    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    @JsonProperty
    public String getEndpoint() {
        return endpoint;
    }

    @JsonProperty
    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @JsonProperty
    public int getBufferSize() {
        return bufferSize;
    }

    @JsonProperty
    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @JsonProperty
    public int getDeliveryThreads() {
        return deliveryThreads;
    }

    @JsonProperty
    public void setDeliveryThreads(int deliveryThreads) {
        this.deliveryThreads = deliveryThreads;
    }

    @JsonProperty
    public int getFlushPeriodSeconds() {
        return flushPeriodSeconds;
    }

    @JsonProperty
    public void setFlushPeriodSeconds(int flushPeriodSeconds) {
        this.flushPeriodSeconds = flushPeriodSeconds;
    }

    public SendService buildKinesisSendService(Environment environment, String name) {
        environment.getName();
        final String healthCheckName = "KinesisStreamHealthCheck";
        final AmazonKinesis kinesisClient = KinesisClientUtil.createKinesisClient(region, endpoint);
        final KinesisHealthCheck healthCheck = new KinesisHealthCheck(kinesisClient, streamName);

        environment.healthChecks().register(healthCheckName, healthCheck);

        final ExecutorService deliveryExecutor = environment.lifecycle()
                .executorService(name + "-batch-send")
                .maxThreads(deliveryThreads)
                .build();

        final ScheduledExecutorService flushExecutor = environment.lifecycle()
                .scheduledExecutorService(name + "-flush")
                .threads(1)
                .build();

        return new KinesisSendService(
                kinesisClient,
                streamName,
                bufferSize,
                flushPeriodSeconds, deliveryExecutor,
                flushExecutor
        );
    }



}
