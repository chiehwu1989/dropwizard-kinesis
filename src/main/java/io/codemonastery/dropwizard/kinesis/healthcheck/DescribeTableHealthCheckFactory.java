package io.codemonastery.dropwizard.kinesis.healthcheck;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.Valid;

public class DescribeTableHealthCheckFactory implements DynamoDbClientHealthCheckFactory {

    @Valid
    @NotEmpty
    private String tableName = "test";

    @Valid
    private boolean tableMustExist = false;

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonIgnore
    public DescribeTableHealthCheckFactory tableName(String tableName) {
        this.setTableName(tableName);
        return this;
    }

    @JsonProperty
    public boolean isTableMustExist() {
        return tableMustExist;
    }

    @JsonProperty
    public void setTableMustExist(boolean tableMustExist) {
        this.tableMustExist = tableMustExist;
    }

    @JsonIgnore
    public DescribeTableHealthCheckFactory tableMustExist(boolean tableMustExist) {
        this.setTableMustExist(tableMustExist);
        return this;
    }

    @Override
    public HealthCheck build(AmazonDynamoDB client) {
        Preconditions.checkNotNull(client, "client cannot be null");
        Preconditions.checkNotNull(getTableName(), "tableName must be specified");
        return new MyHealthCheck(client, getTableName(), isTableMustExist());
    }

    private static class MyHealthCheck extends HealthCheck {
        private final AmazonDynamoDB client;
        private final String tableName;
        private final boolean tableMustExist;

        public MyHealthCheck(AmazonDynamoDB client, String tableName, boolean tableMustExist) {
            this.client = client;
            this.tableName = tableName;
            this.tableMustExist = tableMustExist;
        }

        @Override
        protected Result check() throws Exception {
            Result result = Result.healthy();
            try {
                client.describeTable(tableName);
            } catch (ResourceNotFoundException e) {
                if (tableMustExist) {
                    String message = String.format(
                            "table %s does not exist, was configured to require table existing",
                            tableName);
                    result = Result.unhealthy(message);
                }
            } catch (Exception e) {
                result = Result.unhealthy(e);
            }
            return result;
        }
    }
}
