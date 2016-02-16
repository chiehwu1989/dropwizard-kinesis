package io.codemonastery.dropwizard.kinesis.producer;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Joiner;

import java.util.List;

public class ProducerHealthCheck extends HealthCheck {

    private final ProducerMetrics metrics;

    public ProducerHealthCheck(ProducerMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    protected Result check() throws Exception {
        Result result;

        List<String> failed = metrics.highFailureMetrics();
        if(failed == null || failed.size() == 0){
            result = Result.healthy();
        }else{
            result = Result.unhealthy(Joiner.on(", ").join(failed));
        }

        return result;
    }
}
