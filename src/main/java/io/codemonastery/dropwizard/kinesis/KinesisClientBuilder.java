package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.health.HealthCheckRegistry;
import io.codemonastery.dropwizard.kinesis.healthcheck.KinesisClientHealthCheck;
import io.codemonastery.dropwizard.kinesis.metric.RequestMetricNameStrategy;
import io.codemonastery.dropwizard.kinesis.metric.RequestMetricsNameStrategies;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;

import javax.annotation.Nullable;

public class KinesisClientBuilder {

    private RequestMetricNameStrategy requestMetricNameStrategy = RequestMetricsNameStrategies.METHOD_ONLY;
    private ClientConfiguration clientConfiguration = new ClientConfiguration();

    public KinesisClientBuilder setRequestMetricNameStrategy(@Nullable RequestMetricNameStrategy requestMetricNameStrategy) {
        this.requestMetricNameStrategy = requestMetricNameStrategy;
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
        RequestMetricCollector requestMetricCollector = null;
        if(metrics != null && requestMetricNameStrategy != null){
            requestMetricCollector = new RequestMetricCollector(metrics, name, requestMetricNameStrategy);
        }

        AmazonKinesisClient client = new AmazonKinesisClient(
                credentialsProvider,
                clientConfiguration,
                requestMetricCollector);


        if(lifecycle != null){
            lifecycle.manage(new Managed() {
                @Override
                public void start() throws Exception {

                }

                @Override
                public void stop() throws Exception {
                    client.shutdown();
                }
            });
        }
        if(healthChecks != null){
            healthChecks.register(name, new KinesisClientHealthCheck(client));
        }
        return client;
    }

    private static class RequestMetricCollector extends com.amazonaws.metrics.RequestMetricCollector {

        private final MetricRegistry metrics;
        private final String name;
        private final RequestMetricNameStrategy nameStrategy;

        public RequestMetricCollector(MetricRegistry metrics, String name, RequestMetricNameStrategy nameStrategy) {
            this.metrics = metrics;
            this.name = name;
            this.nameStrategy = nameStrategy;
        }

        @Override
        public void collectMetrics(Request<?> request, Response<?> response) {
            final Timer.Context timerContext = timer(request).time();
        }

        private Timer timer(Request request) {
            return metrics.timer(nameStrategy.getNameFor(name, request));
        }
    }
}
