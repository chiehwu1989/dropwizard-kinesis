package io.codemonastery.dropwizard.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public final class EventObjectMapper<E> implements EventEncoder<E>, EventDecoder<E> {

    private final ObjectMapper objectMapper;
    private final Class<E> klass;

    public EventObjectMapper(ObjectMapper objectMapper, @Nullable Class<E> klass) {
        Preconditions.checkNotNull(objectMapper);
        this.objectMapper = objectMapper;
        this.klass = klass;
    }

    @Nullable
    @Override
    public E decode(ByteBuffer bytes) throws Exception {
        if(klass == null){
            throw new UnsupportedOperationException("cannot decode if event class was not specified");
        }
        return objectMapper.readValue(bytes.array(), klass);
    }

    @Nullable
    @Override
    public byte[] encode(E event) throws Exception {
        return objectMapper.writeValueAsBytes(event);
    }
}
