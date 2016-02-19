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

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

public final class BufferedProducer<E> extends Producer<E> {

    private static final Logger LOG = LoggerFactory.getLogger(BufferedProducer.class);

    private final AmazonKinesis kinesis;
    private final String streamName;

    private final ExecutorService deliveryExecutor;
    private final BufferedProducerMetrics bufferedMetrics;

    private final PutRecordsBuffer buffer;

    public BufferedProducer(AmazonKinesis kinesis,
                            String streamName,
                            Function<E, String> partitionKeyFn,
                            EventEncoder<E> encoder,
                            int maxBufferSize,
                            ScheduledExecutorService deliveryExecutor,
                            BufferedProducerMetrics metrics) {
        super(partitionKeyFn, encoder, metrics);

        Preconditions.checkNotNull(kinesis, "kinesis cannot be null");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(streamName), "must have a stream name");
        Preconditions.checkArgument(maxBufferSize > 0, "maxBufferSize must be positive");
        Preconditions.checkNotNull(deliveryExecutor, "must have a delivery executor");

        this.kinesis = kinesis;
        this.streamName = streamName;
        this.deliveryExecutor = deliveryExecutor;
        this.bufferedMetrics = metrics;

        this.buffer = new PutRecordsBuffer(maxBufferSize);
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
        if(records != null && !records.isEmpty()){
            int failedCount = records.size();
            try(Closeable ignored = metrics.time()) {
                PutRecordsResult result = kinesis.putRecords(new PutRecordsRequest()
                        .withRecords(records)
                        .withStreamName(streamName));
                failedCount = result.getFailedRecordCount();
                if (LOG.isDebugEnabled()) {
                    String message = String.format("Put %d records to stream %s, %d failed",
                            result.getRecords().size(),
                            streamName,
                            failedCount);
                    LOG.debug(message);
                }
            } catch (Exception e) {
                LOG.error("Unexpected error while putting records", e);
            }finally {
                metrics.sent(records.size()-failedCount, failedCount);
            }
        }
    }
}
