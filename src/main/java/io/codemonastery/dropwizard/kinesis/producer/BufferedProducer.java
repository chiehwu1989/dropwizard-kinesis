package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

public class BufferedProducer<E> extends Producer<E> {

    private static final Logger LOG = LoggerFactory.getLogger(BufferedProducer.class);

    private final AmazonKinesis kinesis;
    private final String streamName;

    private final int maxBufferSize;

    private final ExecutorService deliveryExecutor;
    private final List<PutRecordsRequestEntry> buffer;


    public BufferedProducer(AmazonKinesis kinesis,
                            String streamName,
                            Function<E, String> partitionKeyFn,
                            EventEncoder<E> encoder,
                            int maxBufferSize,
                            ScheduledExecutorService deliveryExecutor) {
        super(partitionKeyFn, encoder);

        Preconditions.checkNotNull(kinesis, "kinesis cannot be null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(streamName), "must have a stream name");
        Preconditions.checkArgument(maxBufferSize > 0, "maxBufferSize must be positive");
        Preconditions.checkNotNull(deliveryExecutor, "must have a delivery executor");


        this.kinesis = kinesis;
        this.streamName = streamName;
        this.maxBufferSize = maxBufferSize;
        this.deliveryExecutor = deliveryExecutor;

        this.buffer = new ArrayList<>(maxBufferSize);
    }

    public void flush() {
        try {
            List<PutRecordsRequestEntry> submitMe = null;
            synchronized (buffer) {
                if (buffer.size() > 0) {
                    submitMe = new ArrayList<>(buffer);
                    buffer.clear();
                }
            }
            if (submitMe != null && !submitMe.isEmpty()) {
                final List<PutRecordsRequestEntry> temp = submitMe;
                deliveryExecutor.submit(() -> putRecords(temp));
            }
        } catch (Exception e) {
            LOG.error("unexpected error while flushing", e);
        }
    }

    @Override
    protected void send(PutRecordsRequestEntry record) {
        List<PutRecordsRequestEntry> submitMe = null;
        synchronized (buffer) {
            if (buffer.size() >= maxBufferSize) {
                submitMe = new ArrayList<>(buffer);
                buffer.clear();

            }
            buffer.add(record);
        }
        if (submitMe != null) {
            final List<PutRecordsRequestEntry> temp = submitMe;
            deliveryExecutor.submit(() -> putRecords(temp));
        }
    }

    private void putRecords(List<PutRecordsRequestEntry> records) {
        try {
            PutRecordsResult result = kinesis.putRecords(new PutRecordsRequest()
                    .withRecords(records)
                    .withStreamName(streamName));
            if (LOG.isDebugEnabled()) {
                String message = String.format("Put %d records to stream %s, %d failed",
                        result.getRecords().size(),
                        streamName,
                        Optional.ofNullable(result.getFailedRecordCount()).orElse(0));
                LOG.debug(message);
            }
        } catch (Exception e) {
            LOG.error("Unexpected error while putting records", e);
        }
    }
}