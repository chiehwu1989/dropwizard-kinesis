package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import io.codemonastery.dropwizard.kinesis.producer.PutterMetrics;
import io.codemonastery.dropwizard.kinesis.producer.RecordPutter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RateLimitedRecordPutter implements RecordPutter {

    private static final Logger LOG = LoggerFactory.getLogger(RateLimitedRecordPutter.class);

    private final AmazonKinesis kinesis;
    private final PutterMetrics metrics;
    private final AcquireLimiter limiter;

    public RateLimitedRecordPutter(AmazonKinesis kinesis, PutterMetrics metrics, AcquireLimiter limiter) {
        this.kinesis = kinesis;
        this.metrics = metrics;
        this.limiter = limiter;
    }

    @Override
    public int send(PutRecordsRequest request) throws Exception {
        int failedCount = 0;
        try (Closeable ignored = metrics.time()) {
            boolean success = false;
            //ok to retry indefinitely, only catching rate limit exceptions
            while (!success) {
                int numRecordsSent = request.getRecords().size();
                int numRecordsRateExceeded = 0;
                try {
                    limiter.acquire(request.getRecords().size());
                    PutRecordsResult result = kinesis.putRecords(request);
                    int requestFailedCount = Optional.ofNullable(result.getFailedRecordCount()).orElse(0);
                    if (requestFailedCount == 0) {
                        success = true;
                    } else {
                        List<PutRecordsRequestEntry> newRecordsFromBackoff = new ArrayList<>(result.getRecords().size());
                        List<PutRecordsRequestEntry> oldRecords = request.getRecords();
                        for (int i = 0; i < result.getRecords().size(); i++) {
                            PutRecordsResultEntry recordResult = result.getRecords().get(i);
                            if ("ProvisionedThroughputExceededException".equals(recordResult.getErrorCode())) {
                                newRecordsFromBackoff.add(oldRecords.get(i));
                            }
                        }

                        if (newRecordsFromBackoff.isEmpty()) {
                            success = true;
                        } else {
                            numRecordsRateExceeded = newRecordsFromBackoff.size();
                            request.setRecords(newRecordsFromBackoff);
                        }
                        failedCount += requestFailedCount - numRecordsRateExceeded;
                    }
                    limiter.update(numRecordsSent, numRecordsRateExceeded);
                } catch (ProvisionedThroughputExceededException e) {
                    if (LOG.isDebugEnabled()) {
                        String message = String.format("Exceeded rate limit for stream \"%s\", backing off",
                                request.getStreamName());
                        LOG.debug(message, e);
                    }
                    limiter.update(request.getRecords().size(), request.getRecords().size());
                }
            }
        } finally {
            metrics.sent(request.getRecords().size() - failedCount, failedCount);
        }
        return failedCount;
    }
}
