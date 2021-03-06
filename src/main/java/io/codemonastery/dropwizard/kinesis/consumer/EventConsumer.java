package io.codemonastery.dropwizard.kinesis.consumer;

public interface EventConsumer<E> {

    /**
     * Callback to process an event. If an exception is thrown or false is returned the event will not be consumed.
     * @param event event to process
     * @return true to advance the stream iterator, false to not advance stream iterator
     * @throws Exception you should handle your own exceptions and true/false appropriately,
     *  but we will assume false if you throw an exception
     */
    boolean consume(E event) throws Exception;
}
