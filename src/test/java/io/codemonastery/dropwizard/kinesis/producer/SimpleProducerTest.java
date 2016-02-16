package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import io.codemonastery.dropwizard.kinesis.EventObjectMapper;
import io.codemonastery.dropwizard.kinesis.KinesisResults;
import io.dropwizard.jackson.Jackson;
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

public class SimpleProducerTest {

    private static final EventObjectMapper<String> ENCODER = new EventObjectMapper<>(Jackson.newObjectMapper());

    private static final String STREAM_NAME = "test-stream";

    @Mock
    private AmazonKinesis kinesis;

    private List<PutRecordsRequest> putRecordRequests;

    private Producer<String> producer;

    @Before
    public void setUp() throws Exception {
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

        producer = new SimpleProducerFactory<String>()
                .encoder(ENCODER)
                .streamName(STREAM_NAME)
                .build(null, kinesis, "test-producer");
    }

    @Test
    public void sendOne() throws Exception {
        producer.send("abc");

        assertThat(putRecordRequests.size()).isEqualTo(1);

        PutRecordsRequest request = putRecordRequests.get(0);
        assertThat(request.getRecords().size()).isEqualTo(1);

        PutRecordsRequestEntry firstRecord = request.getRecords().get(0);
        assertThat(ENCODER.decode(firstRecord.getData())).isEqualTo("abc");
        assertThat(firstRecord.getPartitionKey()).isEqualTo("abc");
    }
}
