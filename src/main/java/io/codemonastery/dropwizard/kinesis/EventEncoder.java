package io.codemonastery.dropwizard.kinesis;

import javax.annotation.Nullable;

public interface EventEncoder<E> {

    @Nullable byte[] encode(E event) throws Exception;

}
