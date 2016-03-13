package io.codemonastery.dropwizard.kinesis.producer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;


public class SingletonBlockOnSubmitQueueTest {

    public static final Runnable RUNNABLE_1 = () -> {
    };

    public static final Runnable RUNNABLE_2 = () -> {
    };

    @Rule
    public final TestRule TIMEOUT = new Timeout(10, TimeUnit.SECONDS);

    @Test
    public void offerOne() throws Exception {
        SingletonBlockOnSubmitQueue queue = new SingletonBlockOnSubmitQueue();
        assertThat(queue.offer(RUNNABLE_1)).isTrue();
        assertThat(queue.poll()).isSameAs(RUNNABLE_1);
        assertThat(queue.poll()).isNull();
    }

    @Test
    public void secondOfferBlocks() throws Exception {
        SingletonBlockOnSubmitQueue queue = new SingletonBlockOnSubmitQueue();
        assertThat(queue.offer(RUNNABLE_1)).isTrue();
        new Thread(){
            {
                setDaemon(true);
            }
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    //ignore
                }
                queue.poll();
            }
        }.start();
        long start = System.currentTimeMillis();
        assertThat(queue.offer(RUNNABLE_2)).isTrue();
        double duration = System.currentTimeMillis() - start;
        assertThat(queue.poll()).isSameAs(RUNNABLE_2);
        assertEquals(1000, duration, 50);
        assertThat(queue.poll()).isNull();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void offerWithTimeoutUnsupported() throws Exception {
        new SingletonBlockOnSubmitQueue().offer(RUNNABLE_1, 1, TimeUnit.SECONDS);
    }
}
