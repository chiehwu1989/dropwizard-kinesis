package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

public class ProducerTest {

    public static final EventEncoder<byte[]> ENCODER = new EventEncoder<byte[]>() {
        @Nullable
        @Override
        public byte[] encode(byte[] event) throws Exception {
            return event;
        }
    };

    @Test
    public void eventIsSmallEnough() throws Exception {
        AtomicBoolean innerSendCalled = new AtomicBoolean(false);
        Producer<byte[]> producer = new Producer<byte[]>(b -> "", ENCODER, ProducerMetrics.noOp()) {

            @Override
            protected void send(PutRecordsRequestEntry record) throws Exception {
                innerSendCalled.set(true);
            }
        };
        byte[] almostTooLarge = new byte[Producer.MAX_RECORD_SIZE];
        producer.send(almostTooLarge);
        assertThat(innerSendCalled.get()).isTrue();
    }

    @Test
     public void skipEventBecauseTooLarge() throws Exception {
        AtomicBoolean innerSendCalled = new AtomicBoolean(false);
        Producer<byte[]> producer = new Producer<byte[]>(b -> "", ENCODER, ProducerMetrics.noOp()) {

            @Override
            protected void send(PutRecordsRequestEntry record) throws Exception {
                innerSendCalled.set(true);
            }
        };
        byte[] tooLarge = new byte[Producer.MAX_RECORD_SIZE + 1];
        producer.send(tooLarge);
        assertThat(innerSendCalled.get()).isFalse();
    }

    @Test
     public void encodeFailed() throws Exception {
        AtomicBoolean innerSendCalled = new AtomicBoolean(false);
        Producer<byte[]> producer = new Producer<byte[]>(b -> "", new EventEncoder<byte[]>() {
            @Nullable
            @Override
            public byte[] encode(byte[] event) throws Exception {
                throw new Exception("Oh noes");
            }
        }, ProducerMetrics.noOp()) {

            @Override
            protected void send(PutRecordsRequestEntry record) throws Exception {
                innerSendCalled.set(true);
            }
        };
        producer.send(new byte[1]);
        assertThat(innerSendCalled.get()).isFalse();
    }

    @Test
    public void partitionKeyFailed() throws Exception {
        AtomicBoolean innerSendCalled = new AtomicBoolean(false);
        Producer<byte[]> producer = new Producer<byte[]>(b -> {
            throw new RuntimeException("oh noes again");
        }, ENCODER, ProducerMetrics.noOp()) {

            @Override
            protected void send(PutRecordsRequestEntry record) throws Exception {
                innerSendCalled.set(true);
            }
        };
        producer.send(new byte[1]);
        assertThat(innerSendCalled.get()).isFalse();
    }

    @Test(expected = IllegalStateException.class)
    public void cannotSendAfterStopped() throws Exception {
        Producer<byte[]> producer = new Producer<byte[]>(
                b -> "",
                ENCODER,
                ProducerMetrics.noOp()) {

            @Override
            protected void send(PutRecordsRequestEntry record) throws Exception {
            }
        };
        producer.stop();
        producer.send(new byte[1]);
    }
}
