package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.codemonastery.dropwizard.kinesis.healthcheck.DynamoDbClientHealthCheck;
import io.codemonastery.dropwizard.kinesis.lifecycle.ManagedDynamoDbClient;
import io.codemonastery.dropwizard.kinesis.metric.ClientMetricsProxyFactory;
import io.codemonastery.dropwizard.kinesis.metric.DynamoDbMetricsProxy;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;

import javax.validation.constraints.NotNull;

public class DynamoDbClientBuilder {

    @NotNull
    private Regions region = Regions.DEFAULT_REGION;

    private ClientMetricsProxyFactory<AmazonDynamoDB> metricsProxyFactory = (metrics, dynamoDB, name) -> new DynamoDbMetricsProxy(dynamoDB, metrics, name);

    private ClientConfiguration clientConfiguration = new ClientConfiguration();

    @JsonProperty
    public Regions getRegion() {
        return region;
    }

    @JsonProperty
    public void setRegion(Regions region) {
        this.region = region;
    }

    @JsonIgnore
    public DynamoDbClientBuilder metricsProxy(ClientMetricsProxyFactory<AmazonDynamoDB> metricsProxyFactory) {
        this.metricsProxyFactory = metricsProxyFactory;
        return this;
    }

    @JsonIgnore
    public DynamoDbClientBuilder clientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
        return this;
    }

    @JsonIgnore
    public AmazonDynamoDB build(Environment environment, AWSCredentialsProvider credentialsProvider, String name){
        return build(environment == null ? null : environment.metrics(),
                environment == null ? null : environment.healthChecks(),
                environment == null ? null : environment.lifecycle(),
                credentialsProvider,
                name);
    }

    @JsonIgnore
    public AmazonDynamoDB build(MetricRegistry metrics,
                                HealthCheckRegistry healthChecks,
                                LifecycleEnvironment lifecycle,
                                AWSCredentialsProvider credentialsProvider,
                                String name) {
        AmazonDynamoDB client = makeClient(credentialsProvider);

        if(metrics != null  && metricsProxyFactory != null){
            client = metricsProxyFactory.proxy(metrics, client, name);
        }
        if(healthChecks != null){
            healthChecks.register(name, new DynamoDbClientHealthCheck(client));
        }
        if(lifecycle != null){
            lifecycle.manage(new ManagedDynamoDbClient(client));
        }
        return client;
    }

    private AmazonDynamoDBClient makeClient(AWSCredentialsProvider credentialsProvider) {
        final AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentialsProvider, clientConfiguration);
        if(region != null){
            client.withRegion(region);
        }
        return client;
    }
}
