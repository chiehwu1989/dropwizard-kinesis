package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import io.codemonastery.dropwizard.kinesis.DynamicRateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Optional;

public class RateLimitedRecordPutter implements RecordPutter {

    private static final Logger LOG = LoggerFactory.getLogger(RateLimitedRecordPutter.class);

    private final AmazonKinesis kinesis;
    private final PutterMetrics metrics;
    private final DynamicRateLimiter recordRateLimiter;

    public RateLimitedRecordPutter(AmazonKinesis kinesis, PutterMetrics metrics) {
        this.kinesis = kinesis;
        this.metrics = metrics;
        this.recordRateLimiter = DynamicRateLimiter.create(1000.0, 1.20, 1/60);
    }

    @Override
    public int send(PutRecordsRequest request) throws Exception {
        int failedCount = request.getRecords().size();
        try (Closeable ignored = metrics.time()) {
            boolean success = false;
            //ok to retry indefinitely, only catching rate limit exceptions
            while (!success) {
                try {
                    recordRateLimiter.acquire(request.getRecords().size());
                    PutRecordsResult result = kinesis.putRecords(request);
                    failedCount = Optional.ofNullable(result.getFailedRecordCount()).orElse(0);
                    recordRateLimiter.moveForward();
                    success = true;
                } catch (ProvisionedThroughputExceededException e) {
                    if (LOG.isDebugEnabled()) {
                        String message = String.format("Exceeded rate limit for stream \"%s\", backing off",
                                request.getStreamName());
                        LOG.debug(message, e);
                    }
                    if (e.getMessage() != null) {
                        recordRateLimiter.backOff();
                    }
                }
            }
        } finally {
            metrics.sent(request.getRecords().size() - failedCount, failedCount);
        }
        return failedCount;
    }
}
