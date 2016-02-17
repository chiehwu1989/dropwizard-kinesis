package io.codemonastery.dropwizard.kinesis;

import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class EventObjectMapperTest {

    @Test
    public void decodeEncode() throws Exception {
        EventObjectMapper<Event> objectMapper = new EventObjectMapper<>(Jackson.newObjectMapper(), Event.class);
        Event expected = new Event("a", "b", "c");
        byte[] bytes = objectMapper.encode(expected);
        assertThat(bytes).isNotNull();
        //noinspection ConstantConditions
        Event actual = objectMapper.decode(ByteBuffer.wrap(bytes));
        assertThat(actual).isEqualTo(expected);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void decodeFailsBecauseMissingClass() throws Exception {
        EventObjectMapper<Event> objectMapper = new EventObjectMapper<>(Jackson.newObjectMapper(), null);
        //noinspection ConstantConditions
        objectMapper.decode(ByteBuffer.wrap(objectMapper.encode(new Event("a", "b", "c"))));
    }
}
