package io.codemonastery.dropwizard.kinesis;

import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class EventObjectMapperTest {

    @Test
    public void decodeEncode() throws Exception {
        EventObjectMapper<FakeEvent> objectMapper = new EventObjectMapper<>(Jackson.newObjectMapper(), FakeEvent.class);
        FakeEvent expected = new FakeEvent("a", "b", "c");
        byte[] bytes = objectMapper.encode(expected);
        assertThat(bytes).isNotNull();
        //noinspection ConstantConditions
        FakeEvent actual = objectMapper.decode(ByteBuffer.wrap(bytes));
        assertThat(actual).isEqualTo(expected);
    }

}
