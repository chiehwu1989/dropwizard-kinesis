package io.codemonastery.dropwizard.kinesis.consumer;

import io.codemonastery.dropwizard.kinesis.EventObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordProcessorFactoryTest {

    @Test
    public void newConsumerEachTime() throws Exception {
        EventObjectMapper<String> mapper = new EventObjectMapper<>(Jackson.newObjectMapper(), String.class);
        RecordProcessorFactory<String> factory = new RecordProcessorFactory<>(mapper, () -> event -> true, RecordProcessorMetrics.noOp());
        assertThat(factory.createProcessor()).isNotSameAs(factory.createProcessor());
    }
}
