package io.codemonastery.dropwizard.kinesis.circle;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import io.codemonastery.dropwizard.kinesis.producer.Producer;
import io.codemonastery.dropwizard.kinesis.rule.KinesisClientRule;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;

public class CircleApplication extends Application<CircleConfiguration> {

    public static void main(String[] args) throws Exception {
        new CircleApplication().run(args);
    }

    @Override
    public void run(CircleConfiguration configuration, Environment environment) throws Exception {
       try{
           DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();

           final AmazonKinesis kinesis = configuration.getKinesis()
                   .build(environment, credentialsProvider, "kinesis");

           final AmazonDynamoDB dynamoDb = configuration.getDynamoDb()
                   .build(environment, credentialsProvider, "dynamoDb");

           final Producer<String> producer = configuration.getProducer()
                   .build(environment, kinesis, "circle-producer");

           final CircleResource circleResource = new CircleResource(producer);
           environment.jersey().register(circleResource);

        configuration.getConsumer()
                .consumer(() -> event -> {
                    circleResource.seen(event);
                    return true;
                })
                .build(environment, kinesis, dynamoDb, "circle-consumer");
       }catch (Exception e){
           e.printStackTrace();
           System.exit(0); // see https://github.com/dropwizard/dropwizard/issues/1460
       }
    }
}
