package io.codemonastery.dropwizard.kinesis.lifecycle;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.Managed;

public class ManagedDynamoDbClient implements Managed {

    private AmazonDynamoDB client;

    public ManagedDynamoDbClient(AmazonDynamoDB client) {
        Preconditions.checkNotNull(client, "client cannot be null");
        this.client = client;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
        client.shutdown();
    }
}
