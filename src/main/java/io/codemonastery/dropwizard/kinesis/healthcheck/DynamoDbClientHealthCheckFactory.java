package io.codemonastery.dropwizard.kinesis.healthcheck;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.codahale.metrics.health.HealthCheck;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type",
        defaultImpl = DescribeTableHealthCheckFactory.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ListTablesHealthCheckFactory.class, name = "list_tables"),
        @JsonSubTypes.Type(value = DescribeTableHealthCheckFactory.class, name = "describe_table")
})
public interface DynamoDbClientHealthCheckFactory {

    HealthCheck build(AmazonDynamoDB client);
}
