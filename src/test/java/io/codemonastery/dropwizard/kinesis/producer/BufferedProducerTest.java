package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import io.codemonastery.dropwizard.kinesis.Assertions;
import io.codemonastery.dropwizard.kinesis.EventObjectMapper;
import io.codemonastery.dropwizard.kinesis.KinesisResults;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.util.Duration;
import org.eclipse.jetty.util.component.LifeCycle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class BufferedProducerTest {

    private static final EventObjectMapper<String> ENCODER = new EventObjectMapper<>(Jackson.newObjectMapper(), String.class);

    private static final String STREAM_NAME = "test-stream";
    public static final int MAX_BUFFER_SIZE = 10;
    public static final Duration FLUSH_PERIOD = Duration.milliseconds(100);

    @Mock
    private AmazonKinesis kinesis;

    private List<PutRecordsRequest> putRecordRequests;

    private Producer<String> producer;
    private LifecycleEnvironment lifecycle;

    @Before
    public void setUp() throws Exception {
        lifecycle = new LifecycleEnvironment();
        putRecordRequests = new ArrayList<>();

        initMocks(this);
        when(kinesis.describeStream(STREAM_NAME)).thenReturn(KinesisResults.activeStream(STREAM_NAME));
        when(kinesis.putRecords(any())).then(invocationOnMock -> {
            PutRecordsRequest request = (PutRecordsRequest) invocationOnMock.getArguments()[0];
            putRecordRequests.add(request);
            List<PutRecordsResultEntry> resultRecords = request.getRecords()
                    .stream()
                    .map(r -> (PutRecordsResultEntry) null)
                    .collect(Collectors.toList());
            return new PutRecordsResult()
                    .withRecords(resultRecords)
                    .withFailedRecordCount(0);
        });


        producer = new BufferedProducerFactory<String>() {
            {
                setFlushPeriod(FLUSH_PERIOD);
                setMaxBufferSize(MAX_BUFFER_SIZE);
            }
        }.encoder(ENCODER)
                .streamName(STREAM_NAME)
                .build(null, null, lifecycle, kinesis, "test-producer");
    }

    @After
    public void tearDown() throws Exception {
        for (LifeCycle lifeCycle : lifecycle.getManagedObjects()) {
            lifeCycle.stop();
        }
    }

    @Test
    public void noSendNoPutRecords() throws Exception {
        Thread.sleep(500);
        assertThat(putRecordRequests.size()).isEqualTo(0);
    }

    @Test
    public void sendOneFlushedEventually() throws Exception {
        producer.send("abc");

        Assertions.retry(5, FLUSH_PERIOD, () -> {
            assertThat(putRecordRequests.size()).isEqualTo(1);

            PutRecordsRequest request = putRecordRequests.get(0);
            assertThat(request.getRecords().size()).isEqualTo(1);

            PutRecordsRequestEntry firstRecord = request.getRecords().get(0);
            assertThat(ENCODER.decode(firstRecord.getData())).isEqualTo("abc");
            assertThat(firstRecord.getPartitionKey()).isEqualTo("abc");
        });
    }

    @Test
    public void fillBuffer() throws Exception {
        for (int i = 0; i < MAX_BUFFER_SIZE; i++) {
            producer.send(Integer.toString(i));
        }
        Assertions.retry(5, FLUSH_PERIOD, () -> {
            assertThat(putRecordRequests.size()).isEqualTo(1);

            PutRecordsRequest request = putRecordRequests.get(0);
            assertThat(request.getRecords().size()).isEqualTo(MAX_BUFFER_SIZE);

            for (int i = 0; i < MAX_BUFFER_SIZE; i++) {
                String recordId = Integer.toString(i);
                PutRecordsRequestEntry record = request.getRecords().get(i);
                assertThat(ENCODER.decode(record.getData())).isEqualTo(recordId);
                assertThat(record.getPartitionKey()).isEqualTo(recordId);
            }
        });
    }

    @Test
    public void fillBufferCoupleOfTimes() throws Exception {
        for (int i = 0; i < MAX_BUFFER_SIZE * 5; i++) {
            producer.send(Integer.toString(i));
        }
        Assertions.retry(10, FLUSH_PERIOD, () -> {
            assertThat(putRecordRequests.size()).isEqualTo(5);
            putRecordRequests.sort((o1, o2) -> Integer.compare(
                    Integer.parseInt(o1.getRecords().get(0).getPartitionKey()),
                    Integer.parseInt(o2.getRecords().get(0).getPartitionKey())));

            for (int j = 0; j < 5; j++) {
                PutRecordsRequest request = putRecordRequests.get(j);
                assertThat(request.getRecords().size()).isEqualTo(MAX_BUFFER_SIZE);

                for (int i = 0; i < MAX_BUFFER_SIZE; i++) {
                    String recordId = Integer.toString(j * MAX_BUFFER_SIZE + i);
                    PutRecordsRequestEntry record = request.getRecords().get(i);
                    assertThat(ENCODER.decode(record.getData())).isEqualTo(recordId);
                    assertThat(record.getPartitionKey()).isEqualTo(recordId);
                }
            }
        });
    }

    @Test
    public void flushOnShutdown() throws Exception {
        for (int i = 0; i < MAX_BUFFER_SIZE - 1; i++) {
            producer.send(Integer.toString(i));
        }
        producer.stop(); // forces immediate flush

        assertThat(putRecordRequests.size()).isEqualTo(1);
        PutRecordsRequest request = putRecordRequests.get(0);
        assertThat(request.getRecords().size()).isEqualTo(MAX_BUFFER_SIZE - 1);

        for (int i = 0; i < MAX_BUFFER_SIZE - 1; i++) {
            String recordId = Integer.toString(i);
            PutRecordsRequestEntry record = request.getRecords().get(i);
            assertThat(ENCODER.decode(record.getData())).isEqualTo(recordId);
            assertThat(record.getPartitionKey()).isEqualTo(recordId);
        }
    }
}
