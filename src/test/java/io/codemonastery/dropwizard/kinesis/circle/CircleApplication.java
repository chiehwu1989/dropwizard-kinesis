package io.codemonastery.dropwizard.kinesis.circle;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import io.codemonastery.dropwizard.kinesis.KinesisProducer;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class CircleApplication extends Application<CircleConfiguration> {

    public static void main(String[] args) throws Exception {
        new CircleApplication().run(args);
    }

    @Override
    public void run(CircleConfiguration circleConfiguration, Environment environment) throws Exception {
        AmazonKinesis kinesis = circleConfiguration.getKinesis()
                .build(environment, new DefaultAWSCredentialsProviderChain(), "kinesis");
        KinesisProducer<String> producer = circleConfiguration.getProducer()
                .build(environment, kinesis, "circle-producer");

        environment.jersey().register(new CircleResource(producer));
    }
}
