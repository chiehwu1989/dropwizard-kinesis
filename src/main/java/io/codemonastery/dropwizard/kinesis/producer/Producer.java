package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

public abstract class Producer<E> {

    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

    private final EventEncoder<E> encoder;
    private final Function<E, String> partitionKeyFn;
    protected final ProducerMetrics metrics;

    public Producer(Function<E, String> partitionKeyFn, EventEncoder<E> encoder, ProducerMetrics metrics) {
        Preconditions.checkNotNull(encoder, "encoder cannot be null");
        Preconditions.checkNotNull(partitionKeyFn, "partitionKeyFn cannot be null");
        Preconditions.checkNotNull(metrics, "metrics cannot be null");
        this.encoder = encoder;
        this.partitionKeyFn = partitionKeyFn;
        this.metrics = metrics;
    }

    public void sendAll(List<E> events) throws Exception {
        for (E event : events) {
            send(event);
        }
    }

    public final void send(E event) throws Exception {
        byte[] bytes = null;
        try {
            bytes = encoder.encode(event);
            metrics.encoded();
        } catch (Exception e) {
            metrics.encodeFailed();
            LOG.error("could not encode event " + event.toString());
        }

        String partitionKey = null;
        try{
            partitionKey = partitionKeyFn.apply(event);
        }catch (Exception e){
            metrics.partitionKeyFailed();
            LOG.error("Unexpected exception while calculating partition key for event " + event.toString(), e);
        }
        if(partitionKey == null){
            LOG.warn("skipping event " + event + " because partition key could not be calculated or was null");
        } else if(bytes == null){
            LOG.warn("skipping event " + event + " because could not be encoded or was null");
        } else {
            PutRecordsRequestEntry record = new PutRecordsRequestEntry()
                    .withData(ByteBuffer.wrap(bytes))
                    .withPartitionKey(partitionKey);
            record = extra(record, event);
            send(record);
        }
    }

    protected PutRecordsRequestEntry extra(PutRecordsRequestEntry record, @SuppressWarnings("UnusedParameters") E event){
        return record;
    }

    protected abstract void send(PutRecordsRequestEntry record) throws Exception;

}
