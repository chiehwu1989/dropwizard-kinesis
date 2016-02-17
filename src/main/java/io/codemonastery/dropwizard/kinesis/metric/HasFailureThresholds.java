package io.codemonastery.dropwizard.kinesis.metric;

import java.util.List;

public interface HasFailureThresholds {
    List<String> highFailureMetrics();
}
