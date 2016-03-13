package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;


public class FixedAcquireLimiterFactory implements AcquireLimiterFactory {

    @Min(1)
    private double perSecond = 1000.0;

    @JsonProperty
    public double getPerSecond() {
        return perSecond;
    }

    @JsonProperty
    public void setPerSecond(double perSecond) {
        this.perSecond = perSecond;
    }

    @JsonIgnore
    public FixedAcquireLimiterFactory perSecond(double perSecond){
        this.setPerSecond(perSecond);
        return this;
    }

    @Override
    public FixedAcquireLimiter build() {
        return new FixedAcquireLimiter(perSecond);
    }
}
