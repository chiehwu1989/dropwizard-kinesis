package io.codemonastery.dropwizard.kinesis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public final class EventObjectMapper<E> implements EventEncoder<E>, EventDecoder<E> {

    private final ObjectMapper objectMapper;
    private final TypeReference<E> typeReference;

    public EventObjectMapper(ObjectMapper objectMapper) {
        Preconditions.checkNotNull(objectMapper);
        this.objectMapper = objectMapper;
        this.typeReference = new TypeReference<E>(){};
    }

    @Nullable
    @Override
    public E decode(ByteBuffer bytes) throws Exception {
        return objectMapper.readValue(bytes.array(), typeReference);
    }

    @Nullable
    @Override
    public byte[] encode(E event) throws Exception {
        return objectMapper.writeValueAsBytes(event);
    }
}
