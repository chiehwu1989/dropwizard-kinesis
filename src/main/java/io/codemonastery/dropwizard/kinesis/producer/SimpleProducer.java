package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Optional;
import java.util.function.Function;


public class SimpleProducer<E> extends Producer<E> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);

    private final AmazonKinesis kinesis;
    private final String streamName;
    private final SimpleProducerMetrics metrics;

    public SimpleProducer(AmazonKinesis kinesis,
                          String streamName,
                          Function<E, String> partitionKeyFn,
                          EventEncoder<E> encoder,
                          SimpleProducerMetrics metrics) {
        super(partitionKeyFn, encoder);
        Preconditions.checkNotNull(kinesis, "client cannot be null");
        Preconditions.checkNotNull(streamName, "streamName cannot be null");
        Preconditions.checkNotNull(metrics);
        this.kinesis = kinesis;
        this.streamName = streamName;
        this.metrics = metrics;
    }

    @Override
    protected void send(PutRecordsRequestEntry record) throws Exception {
        int failedCount = 1;
        try(Closeable ignored = metrics.time()) {
            PutRecordsResult  result = kinesis.putRecords(new PutRecordsRequest()
                    .withRecords(record)
                    .withStreamName(streamName));
            failedCount = Optional.ofNullable(result.getFailedRecordCount()).orElse(0);
            if (LOG.isDebugEnabled()) {
                String message = String.format("Put %d records to stream %s, %d failed",
                        result.getRecords().size(),
                        streamName,
                        failedCount);
                LOG.debug(message);
            }
        }finally {
            metrics.sent(1, failedCount);
        }
    }
}
