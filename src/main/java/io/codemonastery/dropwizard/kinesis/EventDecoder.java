package io.codemonastery.dropwizard.kinesis;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public interface EventDecoder<E> {

    @Nullable E decode(ByteBuffer bytes) throws Exception;

}
