package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Preconditions;

public class KinesisClientHealthCheck extends HealthCheck {

    private final AmazonKinesis client;

    public KinesisClientHealthCheck(AmazonKinesis client) {
        Preconditions.checkNotNull(client, "client cannot be null");
        this.client = client;
    }

    @Override
    protected Result check() throws Exception {
        Result result;
        try {
            client.listStreams();
            result = Result.healthy();
        } catch (Exception e) {
            result =  HealthCheck.Result.unhealthy(e);
        }
        return result;
    }
}