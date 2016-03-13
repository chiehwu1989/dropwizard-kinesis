package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

public final class BufferedProducer<E> extends Producer<E> {

    private static final Logger LOG = LoggerFactory.getLogger(BufferedProducer.class);

    private final String streamName;

    private final ExecutorService deliveryExecutor;
    private final BufferedProducerMetrics bufferedMetrics;

    private final PutRecordsBuffer buffer;
    private final RateLimitedRecordPutter putter;

    public BufferedProducer(AmazonKinesis kinesis,
                            String streamName,
                            Function<E, String> partitionKeyFn,
                            EventEncoder<E> encoder,
                            int maxBufferSize,
                            ExecutorService deliveryExecutor,
                            BufferedProducerMetrics metrics) {
        super(partitionKeyFn, encoder, metrics);

        Preconditions.checkNotNull(kinesis, "kinesis cannot be null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(streamName), "must have a stream name");
        Preconditions.checkArgument(maxBufferSize > 0, "maxBufferSize must be positive");
        Preconditions.checkNotNull(deliveryExecutor, "must have a delivery executor");

        this.streamName = streamName;
        this.deliveryExecutor = deliveryExecutor;
        this.bufferedMetrics = metrics;

        this.buffer = new PutRecordsBuffer(maxBufferSize);
        this.putter = new RateLimitedRecordPutter(kinesis, metrics);
    }

    public void flush() {
        try {
            List<PutRecordsRequestEntry> submitMe = buffer.drain();
            bufferedMetrics.bufferRemove(submitMe.size());
            if (!submitMe.isEmpty()) {
                final List<PutRecordsRequestEntry> temp = submitMe;
                deliveryExecutor.submit(() -> putRecords(temp));
            }
        } catch (Exception e) {
            LOG.error("unexpected error while flushing", e);
        }
    }

    @Override
    public void stop() throws Exception {
        synchronized (buffer){
            super.stop();
            putRecords(buffer.drain());
        }
    }

    @Override
    protected void send(PutRecordsRequestEntry record) {
        List<PutRecordsRequestEntry> submitMe = buffer.add(record);
        if(submitMe != null){
            bufferedMetrics.bufferRemove(submitMe.size());
        }
        bufferedMetrics.bufferPut(1);
        if (submitMe != null) {
            final List<PutRecordsRequestEntry> temp = submitMe;
            deliveryExecutor.submit(() -> putRecords(temp));
        }
    }

    private void putRecords(List<PutRecordsRequestEntry> records) {
        try {
            if(records != null && !records.isEmpty()){
                PutRecordsRequest request = new PutRecordsRequest()
                        .withRecords(records)
                        .withStreamName(streamName);
                int failedCount = putter.send(request);
                if (LOG.isDebugEnabled()) {
                    String message = String.format("Put %d records to stream %s, %d failed",
                            request.getRecords().size(),
                            streamName,
                            failedCount);
                    LOG.debug(message);
                }
            }
        } catch (Exception e) {
            LOG.error("Unexpected exception putting records", e);
        }
    }
}
