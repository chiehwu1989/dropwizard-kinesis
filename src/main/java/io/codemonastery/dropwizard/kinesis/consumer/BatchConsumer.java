package io.codemonastery.dropwizard.kinesis.consumer;

import java.util.List;

public interface BatchConsumer<E> {

    boolean consume(List<E> batch) throws Exception;

}
