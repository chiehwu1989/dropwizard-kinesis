package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

public class KinesisProducer<E> {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisProducer.class);

    private final AmazonKinesis client;
    private final int maxBufferSize;
    private final String streamName;

    private final ExecutorService deliveryExecutor;
    private final List<PutRecordsRequestEntry> buffer;
    private final EventEncoder<E> encoder;
    private final Function<E, String> partitionKeyFn;

    public KinesisProducer(AmazonKinesis client, String streamName, int maxBufferSize, ScheduledExecutorService deliveryExecutor, EventEncoder<E> encoder, Function<E, String> partitionKeyFn) {
        this.partitionKeyFn = partitionKeyFn;
        Preconditions.checkNotNull(client, "client cannot be null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(streamName), "must have a stream name");
        Preconditions.checkArgument(maxBufferSize > 0, "maxBufferSize must be positive");
        Preconditions.checkNotNull(deliveryExecutor, "must have a delivery executor");
        Preconditions.checkNotNull(encoder, "encoder cannot be null");
        Preconditions.checkNotNull(partitionKeyFn, "partitionKeyFn cannot be null");

        this.client = client;
        this.streamName = streamName;
        this.maxBufferSize = maxBufferSize;
        this.deliveryExecutor = deliveryExecutor;
        this.encoder = encoder;

        this.buffer = new ArrayList<>(maxBufferSize);
    }

    public final void sendAll(List<E> events) {
        for (E event : events) {
            send(event);
        }
    }

    public final void send(E event){
        byte[] bytes = null;
        try {
            bytes = encoder.encode(event);
        } catch (Exception e) {
            LOG.error("could not encode event " + event.toString());
        }

        String partitionKey = null;
        try{
            partitionKey = partitionKeyFn.apply(event);
        }catch (Exception e){
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

    public void flush() {
        try {
            List<PutRecordsRequestEntry> submitMe = null;
            synchronized (buffer){
                if(buffer.size() > 0){
                    submitMe = new ArrayList<>(buffer);
                    buffer.clear();
                }
            }
            if(submitMe != null && !submitMe.isEmpty()) {
                final List<PutRecordsRequestEntry> temp = submitMe;
                deliveryExecutor.submit(() -> putRecords(temp));
            }
        }catch (Exception e) {
            LOG.error("unexpected error while flushing", e);
        }
    }

    protected PutRecordsRequestEntry extra(PutRecordsRequestEntry record, @SuppressWarnings("UnusedParameters") E event){
        return record;
    }

    private void send(PutRecordsRequestEntry record) {
        List<PutRecordsRequestEntry> submitMe = null;
        synchronized (buffer){
            if(buffer.size() >= maxBufferSize){
                submitMe = new ArrayList<>(buffer);
                buffer.clear();

            }
            buffer.add(record);
        }
        if(submitMe != null) {
            final List<PutRecordsRequestEntry> temp = submitMe;
            deliveryExecutor.submit(() -> putRecords(temp));
        }
    }

    private void putRecords(List<PutRecordsRequestEntry> records) {
        try {
            PutRecordsResult putRecordsResult = client.putRecords(new PutRecordsRequest()
                    .withRecords(records)
                    .withStreamName(streamName));
            if(LOG.isDebugEnabled()){
                LOG.debug("Put %d records to stream %s", putRecordsResult.getRecords().size(), streamName);
            }
        } catch (Exception e) {
            LOG.error("Unexpected error while putting records", e);
        }
    }
}
