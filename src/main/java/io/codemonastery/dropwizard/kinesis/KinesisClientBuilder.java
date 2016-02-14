package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.healthcheck.KinesisClientHealthCheck;
import io.codemonastery.dropwizard.kinesis.lifecycle.ManagedKinesisClient;
import io.codemonastery.dropwizard.kinesis.metric.KinesisMetricsProxy;
import io.codemonastery.dropwizard.kinesis.metric.ClientMetricsProxyFactory;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;

import javax.annotation.Nullable;

public class KinesisClientBuilder {

    @JsonProperty
    private Regions region;

    private ClientMetricsProxyFactory<AmazonKinesis> metricsProxy = (metrics, kinesis, name) -> new KinesisMetricsProxy(kinesis, metrics, name);

    private ClientConfiguration clientConfiguration = new ClientConfiguration();

    public Regions getRegion() {
        return region;
    }

    public void setRegion(Regions region) {
        this.region = region;
    }

    public KinesisClientBuilder metricsProxy(ClientMetricsProxyFactory metricsProxy) {
        this.metricsProxy = metricsProxy;
        return this;
    }

    public KinesisClientBuilder clientConfiguration(ClientConfiguration clientConfiguration) {
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

        AmazonKinesis client = makeClient(credentialsProvider);

        if (metrics != null && metricsProxy != null) {
            client = metricsProxy.proxy(metrics, client, name);
            Preconditions.checkNotNull(client, metricsProxy.getClass().getName() + " returned a null client");
        }
        if (lifecycle != null) {
            lifecycle.manage(new ManagedKinesisClient(client));
        }
        if (healthChecks != null) {
            healthChecks.register(name, new KinesisClientHealthCheck(client));
        }
        return client;
    }

    private AmazonKinesis makeClient(AWSCredentialsProvider credentialsProvider) {
        AmazonKinesisClient client = new AmazonKinesisClient(credentialsProvider, clientConfiguration);
        if(region != null){
            client.withRegion(region);
        }
        return client;
    }
}
