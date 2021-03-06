package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.lifecycle.ManagedKinesisClient;
import io.codemonastery.dropwizard.kinesis.metric.ClientMetricsProxyFactory;
import io.codemonastery.dropwizard.kinesis.metric.KinesisMetricsProxy;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class KinesisFactory {

    @NotNull
    private Regions region;

    private ClientMetricsProxyFactory<AmazonKinesis> metricsProxy = KinesisMetricsProxy::new;

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

    @JsonIgnore
    public KinesisFactory region(Regions region) {
        this.setRegion(region);
        return this;
    }

    @JsonIgnore
    public ClientMetricsProxyFactory<AmazonKinesis> getMetricsProxy() {
        return metricsProxy;
    }

    @JsonIgnore
    public void setMetricsProxy(ClientMetricsProxyFactory<AmazonKinesis> metricsProxy) {
        this.metricsProxy = metricsProxy;
    }

    @JsonIgnore
    public KinesisFactory metricsProxy(ClientMetricsProxyFactory<AmazonKinesis> metricsProxy) {
        this.setMetricsProxy(metricsProxy);
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
    public void setClient(ClientConfiguration clientConfiguration) {
        this.client = new JacksonClientConfiguration(clientConfiguration);
    }

    @JsonIgnore
    public KinesisFactory client(JacksonClientConfiguration clientConfiguration) {
        this.setClient(clientConfiguration);
        return this;
    }

    @JsonIgnore
    public KinesisFactory client(ClientConfiguration clientConfiguration) {
        this.setClient(new JacksonClientConfiguration(clientConfiguration));
        return this;
    }

    public AmazonKinesis build(@Nullable final Environment environment,
                               final AWSCredentialsProvider credentialsProvider,
                               final String name) {
        return build(environment == null ? null : environment.metrics(),
                environment == null ? null : environment.lifecycle(),
                credentialsProvider,
                name);
    }

    public AmazonKinesis build(@Nullable final MetricRegistry metrics,
                               @Nullable final LifecycleEnvironment lifecycle,
                               final AWSCredentialsProvider credentialsProvider,
                               final String name) {

        AmazonKinesis client = makeClient(credentialsProvider);

        if (metrics != null && metricsProxy != null) {
            client = metricsProxy.proxy(client, metrics, name);
            Preconditions.checkNotNull(client, metricsProxy.getClass().getName() + " returned a null client");
        }
        if (lifecycle != null) {
            lifecycle.manage(new ManagedKinesisClient(client));
        }
        return client;
    }

    private AmazonKinesis makeClient(AWSCredentialsProvider credentialsProvider) {
        AmazonKinesisClient client = new AmazonKinesisClient(credentialsProvider, this.getClient());
        if(getRegion() != null){
            client.withRegion(getRegion());
        }
        return client;
    }
}
