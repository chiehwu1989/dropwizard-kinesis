package io.codemonastery.dropwizard.kinesis.producer.ratelimit;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.common.util.concurrent.RateLimiter;
import io.codemonastery.dropwizard.kinesis.Assertions;
import io.codemonastery.dropwizard.kinesis.KinesisResults;
import io.codemonastery.dropwizard.kinesis.producer.ProducerMetrics;
import io.codemonastery.dropwizard.kinesis.producer.RecordPutter;
import io.dropwizard.util.Duration;
import org.assertj.core.data.Offset;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;
import org.mockito.Mock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class RateLimitedRecordPutterTest {

    @Rule
    public final TestRule RETRY_BECAUSE_SLEEPS = (statement, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            Assertions.retry(3, Duration.milliseconds(0), statement::evaluate);
        }
    };

    private static final String STREAM_NAME = "test-stream";

    @Mock
    private AmazonKinesis kinesis;

    private RateLimiter expectedRecordLimiter;
    private List<PutRecordsRequestEntry> putRecordRequests;
    private RecordPutter putter;

    @Before
    public void setUp() throws Exception {
        expectedRecordLimiter = RateLimiter.create(Double.MAX_VALUE);
        putRecordRequests = new ArrayList<>();

        initMocks(this);
        when(kinesis.describeStream(STREAM_NAME)).thenReturn(KinesisResults.activeStream(STREAM_NAME));
        when(kinesis.putRecords(any())).then(invocationOnMock -> {
            PutRecordsRequest request = (PutRecordsRequest) invocationOnMock.getArguments()[0];

            int numRecords = request.getRecords().size();
            int ratedExceededCount = 0;
            while(ratedExceededCount < numRecords){
                if(expectedRecordLimiter.tryAcquire(numRecords - ratedExceededCount)){
                    break;
                }
                ratedExceededCount++;
            }

            List<PutRecordsResultEntry> resultRecords = new ArrayList<>();
            for (int i = 0; i < request.getRecords().size(); i++) {
                final PutRecordsResultEntry resultRecord = new PutRecordsResultEntry();
                if(i < numRecords - ratedExceededCount){
                    putRecordRequests.add(request.getRecords().get(i));
                }else{
                    resultRecord.setErrorCode("ProvisionedThroughputExceededException");
                }
                resultRecords.add(resultRecord);
            }
            return new PutRecordsResult()
                    .withRecords(resultRecords)
                    .withFailedRecordCount(ratedExceededCount);
        });

        putter = new RateLimitedRecordPutter(kinesis, ProducerMetrics.noOp(), new DynamicAcquireLimiter(20, 2, 0.01));
    }

    @Test
    public void backoffToRate() throws Exception {
        expectedRecordLimiter = RateLimiter.create(10);

        long start = System.currentTimeMillis();
        int numRecords = 20;
        int numBatches = 5;
        for (int i = 0; i < numBatches; i++) {
            putter.send(request(numRecords/numBatches, 1));
        }
        double durationMs = System.currentTimeMillis() - start;

        assertThat(putRecordRequests.size()).isEqualTo(numRecords);
        //first batch is not rate limited, so expected duration should be based on rate limiting n-1 batches
        double expectedDurationMs = 1000 * (numRecords - numRecords/numBatches)/ expectedRecordLimiter.getRate();
        assertThat(durationMs).isEqualTo(expectedDurationMs, Offset.offset(100.0));
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
