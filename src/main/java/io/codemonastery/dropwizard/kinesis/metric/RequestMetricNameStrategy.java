package io.codemonastery.dropwizard.kinesis.metric;

import com.amazonaws.Request;

public interface RequestMetricNameStrategy {

    String getNameFor(String name, Request request);
}
