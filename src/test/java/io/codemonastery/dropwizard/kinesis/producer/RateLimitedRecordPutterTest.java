package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.google.common.util.concurrent.RateLimiter;
import io.codemonastery.dropwizard.kinesis.KinesisResults;
import org.assertj.core.data.Offset;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RateLimitedRecordPutterTest {

    private static final String STREAM_NAME = "test-stream";

    @Mock
    private AmazonKinesis kinesis;

    private RateLimiter expectedRecordLimiter;
    private List<PutRecordsRequest> putRecordRequests;
    private RecordPutter putter;

    @Before
    public void setUp() throws Exception {
        expectedRecordLimiter = RateLimiter.create(Double.MAX_VALUE);
        putRecordRequests = new ArrayList<>();

        initMocks(this);
        when(kinesis.describeStream(STREAM_NAME)).thenReturn(KinesisResults.activeStream(STREAM_NAME));
        when(kinesis.putRecords(any())).then(invocationOnMock -> {
            PutRecordsRequest request = (PutRecordsRequest) invocationOnMock.getArguments()[0];
            boolean rateExceeded = !expectedRecordLimiter.tryAcquire(request.getRecords().size());
            if (rateExceeded) {
                throw new ProvisionedThroughputExceededException("Rate Exceeded");
            }
            putRecordRequests.add(request);
            List<PutRecordsResultEntry> resultRecords = request.getRecords()
                    .stream()
                    .map(r -> (PutRecordsResultEntry) null)
                    .collect(Collectors.toList());
            return new PutRecordsResult()
                    .withRecords(resultRecords)
                    .withFailedRecordCount(0);
        });

        putter = new RateLimitedRecordPutter(kinesis, ProducerMetrics.noOp());
    }

    @Test
    public void backoffToRate() throws Exception {
        expectedRecordLimiter = RateLimiter.create(10);

        long start = System.currentTimeMillis();
        int numRecords = 20;
        for (int i = 0; i < numRecords; i++) {
            putter.send(request(1, 1));
        }
        double durationMs = System.currentTimeMillis() - start;

        assertThat(numRecords).isEqualTo(putRecordRequests.size());
        assertThat(durationMs).isEqualTo(2000.0, Offset.offset(100.0));
    }

    private static PutRecordsRequest request(int numRecords, int byteSize) {
        int bytesPerRecord = byteSize / numRecords;
        int bytesInFirstRecord = bytesPerRecord + byteSize % numRecords;
        List<PutRecordsRequestEntry> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            int recordByteSize = i == 0 ? bytesInFirstRecord : bytesPerRecord;
            records.add(record(recordByteSize));
        }
        return new PutRecordsRequest().withStreamName(STREAM_NAME).withRecords(records);
    }

    private static PutRecordsRequestEntry record(int recordByteSize) {
        return new PutRecordsRequestEntry().withData(ByteBuffer.allocate(recordByteSize));
    }

}
