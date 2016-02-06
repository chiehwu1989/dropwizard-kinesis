package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class KinesisSendService implements SendService {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisSendService.class);

    private final AmazonKinesis kinesisClient;
    private final int bufferSize;
    private final ExecutorService deliveryExecutor;

    private ByteBuffer buffer;
    private String streamName;


    public KinesisSendService(AmazonKinesis kinesisClient,
                              String streamName,
                              int bufferSize,
                              int flushPeriodSeconds,
                              ExecutorService deliveryExecutor,
                              ScheduledExecutorService flushExecutor) {
        this.kinesisClient = kinesisClient;
        this.streamName = streamName;
        this.bufferSize = bufferSize;
        this.deliveryExecutor = deliveryExecutor;

        buffer = ByteBuffer.allocate(bufferSize);
        flushExecutor.scheduleAtFixedRate(this::flush, flushPeriodSeconds, flushPeriodSeconds, TimeUnit.SECONDS);
    }

    @Override
    public synchronized void send(Event event) {
        if (event != null) {
            Event.addMetaEventData(event);

            final String recordAsString = event.toString();
            LOG.debug("Putting event: " + recordAsString);

            final byte[] recordAsBytes = recordAsString.getBytes(StandardCharsets.UTF_8);
            if (recordAsBytes.length > buffer.remaining()) {
                flush();
            }
            if (recordAsBytes.length > buffer.remaining()) {
                final String message = String.format("record was larger than buffer size, %d > %d",
                        recordAsBytes.length, bufferSize);
                throw new IllegalStateException(message);
            }
            buffer.put(recordAsBytes);
        }
    }

    synchronized void flush() {
        if (!isEmpty()) {
            final ByteBuffer toSend = buffer;
            newBuffer();
            deliveryExecutor.submit(() -> flush(toSend));
        }
    }

    boolean isEmpty() {
        return buffer.remaining() == buffer.capacity();
    }

    void newBuffer() {
        buffer = ByteBuffer.allocate(bufferSize);
    }


    void flush(ByteBuffer buffer) {
        final PutRecordsRequestEntry entry = new PutRecordsRequestEntry().withData(buffer).withPartitionKey("");
        final PutRecordsRequest request = new PutRecordsRequest().withRecords(entry);
        request.setStreamName(streamName);
        kinesisClient.putRecords(request);
    }
}