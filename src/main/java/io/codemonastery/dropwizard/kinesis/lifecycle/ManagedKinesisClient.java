package io.codemonastery.dropwizard.kinesis.lifecycle;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.Managed;

public class ManagedKinesisClient implements Managed {

    private final AmazonKinesis client;

    public ManagedKinesisClient(AmazonKinesis client) {
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
