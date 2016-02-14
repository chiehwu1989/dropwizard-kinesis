package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import io.codemonastery.dropwizard.kinesis.healthcheck.DynamoDbClientHealthCheck;
import io.codemonastery.dropwizard.kinesis.lifecycle.ManagedDynamoDbClient;
import io.codemonastery.dropwizard.kinesis.metric.ClientMetricsProxyFactory;
import io.codemonastery.dropwizard.kinesis.metric.DynamoDbMetricsProxy;
import io.codemonastery.dropwizard.kinesis.metric.KinesisMetricsProxy;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;

public class DynamoDbClientBuilder {

    private ClientMetricsProxyFactory<AmazonDynamoDB> metricsProxyFactory = (metrics, dynamoDB, name) -> new DynamoDbMetricsProxy(dynamoDB, metrics, name);

    private ClientConfiguration clientConfiguration = new ClientConfiguration();

    public DynamoDbClientBuilder setMetricsProxyFactory(ClientMetricsProxyFactory<AmazonDynamoDB> metricsProxyFactory) {
        this.metricsProxyFactory = metricsProxyFactory;
        return this;
    }

    public DynamoDbClientBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
        return this;
    }

    public AmazonDynamoDB build(Environment environment, AWSCredentialsProvider credentialsProvider, String name){
        return build(environment == null ? null : environment.metrics(),
                environment == null ? null : environment.healthChecks(),
                environment == null ? null : environment.lifecycle(),
                credentialsProvider,
                name);
    }

    public AmazonDynamoDB build(MetricRegistry metrics,
                                HealthCheckRegistry healthChecks,
                                LifecycleEnvironment lifecycle,
                                AWSCredentialsProvider credentialsProvider,
                                String name) {
        AmazonDynamoDB client = new AmazonDynamoDBClient(credentialsProvider, clientConfiguration);

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
}
