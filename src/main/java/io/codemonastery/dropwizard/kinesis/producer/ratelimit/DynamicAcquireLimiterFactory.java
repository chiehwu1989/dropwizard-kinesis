package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

public class DynamicAcquireLimiterFactory implements AcquireLimiterFactory {

    @Min(1)
    private double initialPerSecond = 1000.0;

    @JsonProperty
    public double getInitialPerSecond() {
        return initialPerSecond;
    }

    @JsonProperty
    public void setInitialPerSecond(double initialPerSecond) {
        this.initialPerSecond = initialPerSecond;
    }

    @JsonIgnore
    public DynamicAcquireLimiterFactory initialPerSecond(double initialPerSecond) {
        this.setInitialPerSecond(initialPerSecond);
        return this;
    }

    @Override
    public DynamicAcquireLimiter build() {
        return DynamicAcquireLimiter.create(initialPerSecond);
    }
}
