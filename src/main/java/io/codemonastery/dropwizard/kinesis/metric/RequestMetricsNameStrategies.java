package io.codemonastery.dropwizard.kinesis.metric;

import com.amazonaws.Request;
import org.apache.http.client.HttpClient;
import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;

import static com.codahale.metrics.MetricRegistry.name;

public class RequestMetricsNameStrategies {


    public static final RequestMetricNameStrategy METHOD_ONLY =
            new RequestMetricNameStrategy() {
                @Override
                public String getNameFor(String name, Request request) {
                    return name(HttpClient.class,
                            name,
                            methodNameString(request));
                }
            };

    public static final RequestMetricNameStrategy HOST_AND_METHOD =
            new RequestMetricNameStrategy() {
                @Override
                public String getNameFor(String name, Request request) {
                    return name(HttpClient.class,
                            name,
                            request.getEndpoint().getHost(),
                            methodNameString(request));
                }
            };

    public static final RequestMetricNameStrategy QUERYLESS_URL_AND_METHOD =
            new RequestMetricNameStrategy() {
                @Override
                public String getNameFor(String name, Request request) {
                    try {
                        final URIBuilder url = new URIBuilder(request.getEndpoint());
                        return name(HttpClient.class,
                                name,
                                url.removeQuery().build().toString(),
                                methodNameString(request));
                    } catch (URISyntaxException e) {
                        throw new IllegalArgumentException(e);
                    }
                }
            };

    private static String methodNameString(Request request) {
        return request.getHttpMethod().name().toLowerCase() + "-requests";
    }

    private RequestMetricsNameStrategies() {
    }
}
