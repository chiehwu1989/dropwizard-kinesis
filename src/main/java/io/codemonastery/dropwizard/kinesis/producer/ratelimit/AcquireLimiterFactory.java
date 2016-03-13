package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type",
        defaultImpl = DynamicAcquireLimiterFactory.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = FixedAcquireLimiterFactory.class, name = "fixed"),
        @JsonSubTypes.Type(value = DynamicAcquireLimiterFactory.class, name = "dynamic")
})
public interface AcquireLimiterFactory {

    AcquireLimiter build();

}
