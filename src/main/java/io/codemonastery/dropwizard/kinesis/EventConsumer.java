package io.codemonastery.dropwizard.kinesis;

public interface EventConsumer<E> {

    /**
     * Callback to process an event. If an exception is thrown or false is returned the event will not be consumed.
     * @param event event to process
     * @return true if you want to advance the stream, false if you do now
     * @throws Exception you should handle your own exceptions and true/false appropriately,
     *  but we will assume false if you throw an exception
     */
    boolean process(E event) throws Exception;
}
