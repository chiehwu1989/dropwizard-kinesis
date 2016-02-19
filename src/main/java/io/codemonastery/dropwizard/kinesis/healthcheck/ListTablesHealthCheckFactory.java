package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.base.Preconditions;

public class ListTablesHealthCheckFactory implements DynamoDbClientHealthCheckFactory {

    @Override
    public HealthCheck build(AmazonDynamoDB client) {
        Preconditions.checkNotNull(client, "client cannot be null");
        return new HealthCheck(client);
    }

    private static class HealthCheck extends com.codahale.metrics.health.HealthCheck {
        private final AmazonDynamoDB client;

        public HealthCheck(AmazonDynamoDB client) {
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
}
