package io.codemonastery.dropwizard.kinesis.example;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleWorker;
import io.codemonastery.dropwizard.kinesis.Event;
import io.codemonastery.dropwizard.kinesis.producer.Producer;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ExampleApplication extends Application<ExampleConfiguration> {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleApplication.class);

    public static void main(String[] args) throws Exception {
        new ExampleApplication().run(args);
    }

    @Override
    public void run(ExampleConfiguration configuration, Environment environment) throws Exception {
        final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
        final AmazonKinesis kinesis = configuration.getKinesis().build(environment, credentialsProvider, "kinesis");
        final AmazonDynamoDB dynamodb = configuration.getDynamoDb().build(environment, credentialsProvider, "dynamodb");

        final Producer<Event> producer = configuration.getProducer().build(environment, kinesis, "example-producer");
        //noinspection unused
        final SimpleWorker consumer = configuration.getConsumer()
                .consumer(() -> event -> {
                    LOG.info("Consumed an tasty event: " + event.toString());
                    return true;
                })
                .build(environment, kinesis, dynamodb, "example-consumer");

        //this is just trash code to generate fake events and send them via the producer
        ScheduledExecutorService eventGeneratorService = environment.lifecycle()
                .scheduledExecutorService("generate-fake-events")
                .threads(1)
                .build();
        eventGeneratorService.scheduleAtFixedRate(() -> {
            try {
                String now = Long.toString(System.currentTimeMillis());
                producer.send(new Event(now, now, now));
            } catch (Exception e) {
                LOG.error("Could not send event", e);
            }
        }, 1, 1, TimeUnit.SECONDS);
    }
}
