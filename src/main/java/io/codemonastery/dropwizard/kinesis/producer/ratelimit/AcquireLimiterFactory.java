package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "type",
        defaultImpl = DynamicAcquireLimiterFactory.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DynamicAcquireLimiterFactory.class, name = "dynamic"),
        @JsonSubTypes.Type(value = FixedAcquireLimiterFactory.class, name = "fixed"),
        @JsonSubTypes.Type(value = NoLimitAcquireLimiterFactory.class, name = "nolimit")
})
public interface AcquireLimiterFactory {

    AcquireLimiter build();

}
