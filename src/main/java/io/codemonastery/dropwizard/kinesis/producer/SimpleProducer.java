package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;


public class SimpleProducer<E> extends Producer<E> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);

    private final AmazonKinesis kinesis;
    private final String streamName;

    public SimpleProducer(AmazonKinesis kinesis,
                          String streamName,
                          Function<E, String> partitionKeyFn,
                          EventEncoder<E> encoder) {
        super(partitionKeyFn, encoder);
        Preconditions.checkNotNull(kinesis, "client cannot be null");
        Preconditions.checkNotNull(streamName, "streamName cannot be null");
        this.kinesis = kinesis;
        this.streamName = streamName;
    }

    @Override
    protected void send(PutRecordsRequestEntry record) {
        PutRecordsResult result = kinesis.putRecords(new PutRecordsRequest()
                .withRecords(record)
                .withStreamName(streamName));
        if (LOG.isDebugEnabled()) {
            String message = String.format("Put %d records to stream %s, %d failed",
                    result.getRecords().size(),
                    streamName,
                    Optional.ofNullable(result.getFailedRecordCount()).orElse(0));
            LOG.debug(message);
        }
    }
}
