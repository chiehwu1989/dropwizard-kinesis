package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Preconditions;

public class DynamoDbClientHealthCheck extends HealthCheck {

    private final AmazonDynamoDB client;

    public DynamoDbClientHealthCheck(AmazonDynamoDB client) {
        Preconditions.checkNotNull(client, "client cannot be null");
        this.client = client;
    }

    @Override
    protected Result check() throws Exception {
        Result result;
        try{
            client.listTables();
            result = Result.healthy();
        }catch (Exception e){
            result = Result.unhealthy(e);
        }
        return result;
    }
}
