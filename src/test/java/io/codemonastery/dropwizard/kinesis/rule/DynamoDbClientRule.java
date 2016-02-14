package io.codemonastery.dropwizard.kinesis.rule;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.junit.rules.ExternalResource;

import java.util.function.Supplier;

public class DynamoDbClientRule extends ExternalResource implements Supplier<AmazonDynamoDB> {

    private AmazonDynamoDB client;

    @Override
    protected void before() throws Throwable {
        client = new AmazonDynamoDBClient(new DefaultAWSCredentialsProviderChain());
    }

    @Override
    protected void after() {
        client.shutdown();
        client = null;
    }

    public AmazonDynamoDB getClient() {
        if(client == null){
            throw new IllegalStateException("CLIENT_RULE is not initialized, did you use the @ClassRule or @Rule annotations?");
        }
        return client;
    }

    @Override
    public AmazonDynamoDB get() {
        return getClient();
    }
}
