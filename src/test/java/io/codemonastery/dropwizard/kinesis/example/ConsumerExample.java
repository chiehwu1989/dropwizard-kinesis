package io.codemonastery.dropwizard.kinesis.example;


import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import io.codemonastery.dropwizard.kinesis.CredentialUtils;
import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import io.codemonastery.dropwizard.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import io.codemonastery.dropwizard.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import io.codemonastery.dropwizard.kinesis.clientlibrary.lib.worker.Worker;

import java.net.InetAddress;
import java.util.UUID;

/**
 * Sample Amazon Kinesis Application.
 */
public final class ConsumerExample {

//    public static final String SAMPLE_APPLICATION_STREAM_NAME = "test-kinesis-app-stream";
    public static final String SAMPLE_APPLICATION_STREAM_NAME = "test-circle";

    private static final String SAMPLE_APPLICATION_NAME = "test-kinesis-app";

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM =
            InitialPositionInStream.LATEST;

    private static AWSCredentialsProvider credentialsProvider;

    private static void init() {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        credentialsProvider = new CredentialUtils();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load AWS credentials.", e);
        }
    }

    public static void main(String[] args) throws Exception {
        init();

        if (args.length == 1 && "delete-resources".equals(args[0])) {
            deleteResources();
            return;
        }

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
                        SAMPLE_APPLICATION_STREAM_NAME,
                        credentialsProvider,
                        workerId);
        kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);

        IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactoryExample();
        final Worker.Builder builder = new Worker.Builder();
        builder.recordProcessorFactory(recordProcessorFactory);
        builder.config(kinesisClientLibConfiguration);
        final Worker worker = builder.build();

        System.out.printf("Running %s to process stream %s as worker %s...\n",
                SAMPLE_APPLICATION_NAME,
                SAMPLE_APPLICATION_STREAM_NAME,
                workerId);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }

    public static void deleteResources() {
        AWSCredentials credentials = credentialsProvider.getCredentials();

        // Delete the stream
        AmazonKinesis kinesis = new AmazonKinesisClient(credentials);
        System.out.printf("Deleting the Amazon Kinesis stream used by the sample. Stream Name = %s.\n",
                SAMPLE_APPLICATION_STREAM_NAME);
        try {
            kinesis.deleteStream(SAMPLE_APPLICATION_STREAM_NAME);
        } catch (ResourceNotFoundException ex) {
            // The stream doesn't exist.
        }

        // Delete the table
        AmazonDynamoDBClient dynamoDB = new AmazonDynamoDBClient(credentialsProvider.getCredentials());
        System.out.printf("Deleting the Amazon DynamoDB table used by the Amazon Kinesis Client Library. Table Name = %s.\n",
                SAMPLE_APPLICATION_NAME);
        try {
            dynamoDB.deleteTable(SAMPLE_APPLICATION_NAME);
        } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException ex) {
            // The table doesn't exist.
        }
    }
}
