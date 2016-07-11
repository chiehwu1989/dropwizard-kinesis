package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.MetricRegistry;
import io.codemonastery.dropwizard.kinesis.EventDecoder;
import io.codemonastery.dropwizard.kinesis.EventObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class RecordProcessorTest {

    public static final EventObjectMapper<String> MAPPER = new EventObjectMapper<>(Jackson.newObjectMapper(), String.class);

    @Mock
    private IRecordProcessorCheckpointer checkpointer;
    private MetricRegistry metricRegistry;
    private RecordProcessorMetrics metrics;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        metricRegistry = new MetricRegistry();
        metrics = new RecordProcessorMetrics(metricRegistry, "foo");
    }

    @Test
    public void happyPath() throws Exception {
        List<String> actual = new ArrayList<>();
        EventConsumer<String> eventConsumer = event -> {
            actual.add(event);
            return true;
        };

        List<String> expected = Arrays.asList("aaa", "bbb", "ccc");
        List<Record> expectedRecords = records(expected);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer)
                .withMillisBehindLatest(1000L);

        RecordProcessor<String> processor = new RecordProcessor<>(MAPPER, eventConsumer, metrics);
        processor.initialize(new InitializationInput().withShardId("123"));
        processor.processRecords(input);

        assertThat(actual).isEqualTo(expected);
        verify(checkpointer).checkpoint(expectedRecords.get(2));
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.getGauges().containsKey("foo-millis-behind-latest-123")).isTrue();
    }

    @Test
    public void returnsFalse() throws Exception {
        List<String> actual = new ArrayList<>();
        EventConsumer<String> eventConsumer = event -> {
            if("ccc".equals(event)){
                return false;
            }else{
                actual.add(event);
                return true;
            }
        };

        List<String> expected = Arrays.asList("aaa", "bbb", "ccc");
        List<Record> expectedRecords = records(expected);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        RecordProcessor<String> processor = new RecordProcessor<>(MAPPER, eventConsumer, metrics);
        processor.processRecords(input);

        assertThat(actual).isEqualTo(expected.subList(0, 2));
        verify(checkpointer).checkpoint(expectedRecords.get(1));
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(2);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
    }

    @Test
    public void throwsException() throws Exception {
        List<String> actual = new ArrayList<>();
        EventConsumer<String> eventConsumer = event -> {
            if("ccc".equals(event)){
                throw new Exception("cannot process ccc");
            }else{
                actual.add(event);
                return true;
            }
        };

        List<String> expected = Arrays.asList("aaa", "bbb", "ccc");
        List<Record> expectedRecords = records(expected);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        RecordProcessor<String> processor = new RecordProcessor<>(MAPPER, eventConsumer, metrics);
        processor.processRecords(input);

        assertThat(actual).isEqualTo(expected.subList(0, 2));
        verify(checkpointer).checkpoint(expectedRecords.get(1));
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(2);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
    }

    @Test
    public void decodeFailure() throws Exception {
        List<String> actual = new ArrayList<>();
        EventConsumer<String> eventConsumer = event -> {
            if("ccc".equals(event)){
                throw new Exception("cannot process ccc");
            }else{
                actual.add(event);
                return true;
            }
        };

        List<String> expected = Arrays.asList("aaa", "bbb", "ccc");
        List<Record> expectedRecords = records(expected);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        RecordProcessor<String> processor = new RecordProcessor<>(new EventDecoder<String>() {
            @Nullable
            @Override
            public String decode(ByteBuffer bytes) throws Exception {
                String value = MAPPER.decode(bytes);
                if("ccc".equals(value)){
                    throw new Exception("decode failue for ccc");
                }
                return value;
            }
        }, eventConsumer, metrics);
        processor.processRecords(input);

        assertThat(actual).isEqualTo(expected.subList(0, 2));
        verify(checkpointer).checkpoint(expectedRecords.get(1));
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(2);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(2);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(1);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
    }

    @Test
    public void decodeReturnsNull() throws Exception {
        List<String> actual = new ArrayList<>();
        EventConsumer<String> eventConsumer = event -> {
            if("ccc".equals(event)){
                throw new Exception("cannot process ccc");
            }else{
                actual.add(event);
                return true;
            }
        };

        List<String> expected = Arrays.asList("aaa", "bbb", "ccc");
        List<Record> expectedRecords = records(expected);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        RecordProcessor<String> processor = new RecordProcessor<>(new EventDecoder<String>() {
            @Nullable
            @Override
            public String decode(ByteBuffer bytes) throws Exception {
                String value = MAPPER.decode(bytes);
                if("ccc".equals(value)){
                    value = null;
                }
                return value;
            }
        }, eventConsumer, metrics);
        processor.processRecords(input);

        assertThat(actual).isEqualTo(expected.subList(0, 2)); // was not sent to consumer
        verify(checkpointer).checkpoint(expectedRecords.get(2)); //but still counts as processed
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(2);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
    }

    @Test
    public void checkpointerShutdownException() throws Exception {
        doThrow(new ShutdownException("test says we are shutdown"))
                .when(checkpointer).checkpoint(any(Record.class));
        List<Record> expectedRecords = records(Arrays.asList("aaa", "bbb", "ccc"));
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        RecordProcessor<String> processor = new RecordProcessor<>(MAPPER, event -> true, metrics);
        processor.processRecords(input);
        verify(checkpointer).checkpoint(expectedRecords.get(2)); //but still counts as processed
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void checkpointerException() throws Exception {
        doThrow(new RuntimeException("test says we are exceptional"))
                .when(checkpointer).checkpoint(any(Record.class));
        List<Record> expectedRecords = records(Arrays.asList("aaa", "bbb", "ccc"));
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        RecordProcessor<String> processor = new RecordProcessor<>(MAPPER, event -> true, metrics);
        processor.processRecords(input);
        verify(checkpointer).checkpoint(expectedRecords.get(2)); //but still counts as processed
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void startupShutdownMetrics() throws Exception {
        RecordProcessor<String> processor = new RecordProcessor<>(MAPPER, event -> true, metrics);
        assertThat(metricRegistry.counter("foo-processors").getCount()).isEqualTo(0);
        processor.initialize(null);
        assertThat(metricRegistry.counter("foo-processors").getCount()).isEqualTo(1);
        processor.shutdown(null);
        assertThat(metricRegistry.counter("foo-processors").getCount()).isEqualTo(0);
    }

    private List<Record> records(List<String> strings) {
        return strings
                .stream().map(s -> new Record().withData(ByteBuffer.wrap(encodeSilently(s))))
                .collect(Collectors.toList());
    }

    private byte[] encodeSilently(String s) {
        try {
            return MAPPER.encode(s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
