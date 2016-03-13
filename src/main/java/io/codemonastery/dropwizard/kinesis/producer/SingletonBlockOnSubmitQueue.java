package io.codemonastery.dropwizard.kinesis.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

class SingletonBlockOnSubmitQueue extends ArrayBlockingQueue<Runnable> {

    private static final Logger LOG = LoggerFactory.getLogger(SingletonBlockOnSubmitQueue.class);

    SingletonBlockOnSubmitQueue() {
        super(1, true);
    }

    @Override
    public boolean offer(Runnable runnable, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(Runnable runnable) {
        try {
            super.put(runnable);
            return true;
        } catch (InterruptedException e) {
            LOG.error("Could not submit task because interrupted", e);
        }
        return false;
    }
}
