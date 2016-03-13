package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Function;


public final class SimpleProducer<E> extends Producer<E> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);

    private final String streamName;
    private final RateLimitedRecordPutter putter;

    public SimpleProducer(AmazonKinesis kinesis,
                          String streamName,
                          Function<E, String> partitionKeyFn,
                          EventEncoder<E> encoder,
                          ProducerMetrics metrics) {
        super(partitionKeyFn, encoder, metrics);
        Preconditions.checkNotNull(kinesis, "client cannot be null");
        Preconditions.checkNotNull(streamName, "streamName cannot be null");
        this.streamName = streamName;
        this.putter = new RateLimitedRecordPutter(kinesis, metrics);
    }

    @Override
    protected void send(PutRecordsRequestEntry record) throws Exception {
        PutRecordsRequest request = new PutRecordsRequest()
                .withRecords(record)
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
}
