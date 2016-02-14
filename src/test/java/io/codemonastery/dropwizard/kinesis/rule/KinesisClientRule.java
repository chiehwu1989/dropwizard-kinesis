package io.codemonastery.dropwizard.kinesis.rule;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import org.junit.rules.ExternalResource;

import java.util.UUID;
import java.util.function.Supplier;

public class KinesisClientRule extends ExternalResource implements Supplier<AmazonKinesis> {

    private AmazonKinesis client;

    @Override
    protected void before() throws Throwable {
        client = new AmazonKinesisClient(new DefaultAWSCredentialsProviderChain());
    }

    @Override
    protected void after() {
        client.shutdown();
        client = null;
    }

    public AmazonKinesis getClient() {
        if(client == null){
            throw new IllegalStateException("CLIENT_RULE is not initialized, did you use the @ClassRule or @Rule annotations?");
        }
        return client;
    }

    @Override
    public AmazonKinesis get() {
        return getClient();
    }
}
