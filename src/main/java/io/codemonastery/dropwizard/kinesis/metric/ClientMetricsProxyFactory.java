package io.codemonastery.dropwizard.kinesis.metric;

import com.codahale.metrics.MetricRegistry;

public interface ClientMetricsProxyFactory<CLIENT> {

    CLIENT proxy(CLIENT client, MetricRegistry metrics, String name);

}
