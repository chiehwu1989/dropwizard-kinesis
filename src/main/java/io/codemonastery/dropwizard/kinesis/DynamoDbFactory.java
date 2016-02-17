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

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class DynamoDbFactory {

    @NotNull
    private Regions region = Regions.DEFAULT_REGION;

    private ClientMetricsProxyFactory<AmazonDynamoDB> metricsProxyFactory = DynamoDbMetricsProxy::new;

    @Valid
    @NotNull
    private JacksonClientConfiguration client = new JacksonClientConfiguration();

    @JsonProperty
    public Regions getRegion() {
        return region;
    }

    @JsonProperty
    public void setRegion(Regions region) {
        this.region = region;
    }

    public DynamoDbFactory region(Regions region){
        this.setRegion(region);
        return this;
    }

    @JsonIgnore
    public ClientMetricsProxyFactory<AmazonDynamoDB> getMetricsProxyFactory() {
        return metricsProxyFactory;
    }

    @JsonIgnore
    public void setMetricsProxyFactory(ClientMetricsProxyFactory<AmazonDynamoDB> metricsProxyFactory) {
        this.metricsProxyFactory = metricsProxyFactory;
    }
    @JsonIgnore
    public DynamoDbFactory metricsProxy(ClientMetricsProxyFactory<AmazonDynamoDB> metricsProxyFactory) {
        this.setMetricsProxyFactory(metricsProxyFactory);
        return this;
    }

    @JsonProperty
    public JacksonClientConfiguration getClient() {
        return client;
    }

    @JsonProperty
    public void setClient(JacksonClientConfiguration client) {
        this.client = client;
    }

    @JsonIgnore
    public void setClient(ClientConfiguration client) {
        this.client = new JacksonClientConfiguration(client);
    }

    @JsonIgnore
    public DynamoDbFactory client(ClientConfiguration clientConfiguration) {
        this.setClient(clientConfiguration);
        return this;
    }

    @JsonIgnore
    public DynamoDbFactory client(JacksonClientConfiguration clientConfiguration) {
        this.setClient(clientConfiguration);
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
            client = metricsProxyFactory.proxy(client, metrics, name);
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
        final AmazonDynamoDBClient client = new AmazonDynamoDBClient(credentialsProvider, this.client);
        if(region != null){
            client.withRegion(region);
        }
        return client;
    }
}
