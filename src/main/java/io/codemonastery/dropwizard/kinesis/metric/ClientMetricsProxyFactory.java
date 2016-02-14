package io.codemonastery.dropwizard.kinesis.metric;

import com.codahale.metrics.MetricRegistry;

public interface ClientMetricsProxyFactory<CLIENT> {

    CLIENT proxy(MetricRegistry metrics, CLIENT client, String name);

}
