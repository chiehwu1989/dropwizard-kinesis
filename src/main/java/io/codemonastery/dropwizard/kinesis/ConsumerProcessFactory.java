package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import io.codemonastery.dropwizard.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import io.codemonastery.dropwizard.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import io.codemonastery.dropwizard.kinesis.clientlibrary.lib.worker.Worker;
import io.dropwizard.setup.Environment;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsumerProcessFactory {

    @Min(1024)
    @Max(5 * 1024 * 1024)
    protected int bufferSize = 1024 * 1024 * 5;

    @Min(1)
    @Max(500)
    protected int maxElements = 500;


    @Min(1)
    protected int consumerThreads = 10;

    @Min(1)
    protected int flushPeriodSeconds = 60;


    protected Region region = Region.getRegion(Regions.US_EAST_1);

    protected static InitialPositionInStream initialPositionInStream =
            InitialPositionInStream.LATEST;


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
    public int getConsumerThreads() {
        return consumerThreads;
    }

    @JsonProperty
    public void setConsumerThreads(int consumerThreads) {
        this.consumerThreads = consumerThreads;
    }

    @JsonProperty
    public int getFlushPeriodSeconds() {
        return flushPeriodSeconds;
    }

    @JsonProperty
    public void setFlushPeriodSeconds(int flushPeriodSeconds) {
        this.flushPeriodSeconds = flushPeriodSeconds;
    }

    @JsonProperty
    public static InitialPositionInStream getInitialPositionInStream() {
        return initialPositionInStream;
    }

    @JsonProperty
    public void setInitialPositionInStream(String initialPosition) {
        this.initialPositionInStream = InitialPositionInStream.valueOf(initialPosition);
    }


    public Worker buildKinesisConsumerService(String name, Environment environment, IRecordProcessorFactory recordProcessorFactory)  {

        final String healthCheckName = "KinesisConsumerHealthCheck";
        final AmazonKinesis kinesisClient = KinesisClientUtil.createKinesisClient(region, endpoint, name);
        final KinesisHealthCheck healthCheck = new KinesisHealthCheck(kinesisClient, streamName);
        environment.healthChecks().register(healthCheckName, healthCheck);

        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        String workerId = calcWorkerID(name);
        AWSCredentialsProvider credentialsProvider = getCredentialProvider();

        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(
                        name,
                        streamName,
                        credentialsProvider,
                        workerId);
        kinesisClientLibConfiguration.withInitialPositionInStream(initialPositionInStream);

        final Worker.Builder builder = new Worker.Builder();

        final ExecutorService executorService = environment.lifecycle()
                .executorService(name + "-executor")
                .maxThreads(consumerThreads).build();

        builder.recordProcessorFactory(recordProcessorFactory)
                .config(kinesisClientLibConfiguration)
                .execService(executorService);

        final Worker worker = builder.build();

        final ExecutorService shardHandler = environment.lifecycle()
                .executorService(name + "-shard-handler")
                .minThreads(1).maxThreads(1).build();

        shardHandler.submit(worker::run);
        return worker;
    }

    private String calcWorkerID(String name) {
        String ip = null;
        try {
            ip = Stream
                    .of(InetAddress.getLocalHost().getCanonicalHostName(), ":")
                    .filter(s -> s != null)
                    .collect(Collectors.joining());
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return Stream
                .of(name, ":", ip, UUID.randomUUID().toString())
                .filter(s -> s != null)
                .collect(Collectors.joining());
    }

    private CredentialUtils getCredentialProvider() {
        CredentialUtils credentialsProvider;
        credentialsProvider = new CredentialUtils();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load AWS credentials.", e);
        }
        return credentialsProvider;
    }
}
