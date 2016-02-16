package io.codemonastery.dropwizard.kinesis.producer;

import java.io.Closeable;

public final class NoOpClose implements Closeable {

    public static final NoOpClose INSTANCE = new NoOpClose();

    public NoOpClose() {
    }

    @Override
    public void close() {
        //no op!
    }
}
