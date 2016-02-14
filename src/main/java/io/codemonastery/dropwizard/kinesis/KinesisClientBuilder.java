package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.healthcheck.KinesisClientHealthCheck;
import io.codemonastery.dropwizard.kinesis.lifecycle.ManagedKinesisClient;
import io.codemonastery.dropwizard.kinesis.metric.KinesisMetricsProxy;
import io.codemonastery.dropwizard.kinesis.metric.ClientMetricsProxyFactory;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;

import javax.annotation.Nullable;

public class KinesisClientBuilder {

    private ClientMetricsProxyFactory<AmazonKinesis> metricsProxyFactory = (metrics, kinesis, name) -> new KinesisMetricsProxy(kinesis, metrics, name);

    private ClientConfiguration clientConfiguration = new ClientConfiguration();

    public KinesisClientBuilder setMetricsProxyFactory(ClientMetricsProxyFactory metricsProxyFactory) {
        this.metricsProxyFactory = metricsProxyFactory;
        return this;
    }

    public KinesisClientBuilder setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
        return this;
    }

    public AmazonKinesis build(@Nullable final Environment environment,
                               final AWSCredentialsProvider credentialsProvider,
                               final String name) {
        return build(environment == null ? null : environment.metrics(),
                environment == null ? null : environment.healthChecks(),
                environment == null ? null : environment.lifecycle(),
                credentialsProvider,
                name);
    }

    public AmazonKinesis build(@Nullable final MetricRegistry metrics,
                               @Nullable final HealthCheckRegistry healthChecks,
                               @Nullable final LifecycleEnvironment lifecycle,
                               final AWSCredentialsProvider credentialsProvider,
                               final String name) {

        AmazonKinesis client = new AmazonKinesisClient(
                credentialsProvider,
                clientConfiguration);

        if (metrics != null && metricsProxyFactory != null) {
            client = metricsProxyFactory.proxy(metrics, client, name);
            Preconditions.checkNotNull(client, metricsProxyFactory.getClass().getName() + " returned a null client");
        }
        if (lifecycle != null) {
            lifecycle.manage(new ManagedKinesisClient(client));
        }
        if (healthChecks != null) {
            healthChecks.register(name, new KinesisClientHealthCheck(client));
        }
        return client;
    }
}
