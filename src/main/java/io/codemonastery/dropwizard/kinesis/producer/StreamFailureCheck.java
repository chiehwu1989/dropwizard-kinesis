package io.codemonastery.dropwizard.kinesis.producer;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;
import io.codemonastery.dropwizard.kinesis.metric.HasFailureThresholds;

import java.util.List;

public class StreamFailureCheck extends HealthCheck {

    private final HasFailureThresholds metrics;
    private final HealthCheck stream;

    public StreamFailureCheck(HasFailureThresholds metrics, HealthCheck stream) {
        this.stream = stream;
        this.metrics = metrics;
    }

    @Override
    protected Result check() throws Exception {
        Result result = stream.execute();

        //only if we can connect to stream should be check metrics
        if (result.isHealthy()) {
            List<String> failed = metrics.highFailureMetrics();
            if (failed != null && failed.size() > 0) {
                result = Result.unhealthy(Joiner.on(", ").join(failed));
            }
        }

        return result;
    }
}
