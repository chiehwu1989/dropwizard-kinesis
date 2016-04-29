package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
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
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class BatchProcessorTest {

    private static final EventObjectMapper<String> MAPPER = new EventObjectMapper<>(Jackson.newObjectMapper(), String.class);

    @Mock
    private IRecordProcessorCheckpointer checkpointer;

    private BatchProcessorMetrics metrics;

    private MetricRegistry metricRegistry;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        metricRegistry = new MetricRegistry();
        metrics = new BatchProcessorMetrics(metricRegistry, "foo");
    }

    @Test
    public void happyPath() throws Exception {
        List<String> actual = new ArrayList<>();
        BatchConsumer<String> eventConsumer = batch -> {
            actual.addAll(batch);
            return true;
        };

        List<String> expected = Arrays.asList("aaa", "bbb", "ccc");
        List<Record> expectedRecords = records(expected);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        BatchProcessor<String> processor = new BatchProcessor<>(MAPPER, eventConsumer, metrics);
        processor.processRecords(input);

        assertThat(actual).isEqualTo(expected);
        verify(checkpointer).checkpoint();
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
    }

    @Test
    public void returnsFalse() throws Exception {
        BatchConsumer<String> eventConsumer = batch -> false;

        List<String> expected = Arrays.asList("aaa", "bbb", "ccc");
        List<Record> expectedRecords = records(expected);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        BatchProcessor<String> processor = new BatchProcessor<>(MAPPER, eventConsumer, metrics);
        processor.processRecords(input);

        verify(checkpointer, never()).checkpoint();
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
    }

    @Test
    public void throwsException() throws Exception {
        BatchConsumer<String> eventConsumer = batch -> {
            throw new Exception("cannot process batch");
        };

        List<String> expected = Arrays.asList("aaa", "bbb", "ccc");
        List<Record> expectedRecords = records(expected);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        BatchProcessor<String> processor = new BatchProcessor<>(MAPPER, eventConsumer, metrics);
        processor.processRecords(input);

        verify(checkpointer, never()).checkpoint();
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
    }

    @Test
    public void decodeFailure() throws Exception {
        BatchConsumer<String> eventConsumer = batch -> true;

        List<String> expected = Arrays.asList("aaa", "bbb", "ccc");
        List<Record> expectedRecords = records(expected);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        final EventDecoder<String> eventDecoder = new EventDecoder<String>() {
            @Nullable
            @Override
            public String decode(ByteBuffer bytes) throws Exception {
                throw new Exception("decode failure");
            }
        };
        BatchProcessor<String> processor = new BatchProcessor<>(eventDecoder, eventConsumer, metrics);
        processor.processRecords(input);

        verify(checkpointer, never()).checkpoint();
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(1);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(0);
    }

    @Test
    public void decodeReturnsNull() throws Exception {
        BatchConsumer<String> eventConsumer = event -> true;

        List<String> expected = Arrays.asList("aaa", "bbb", "ccc");
        List<Record> expectedRecords = records(expected);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        BatchProcessor<String> processor = new BatchProcessor<>(new EventDecoder<String>() {
            @Nullable
            @Override
            public String decode(ByteBuffer bytes) throws Exception {
                String value = MAPPER.decode(bytes);
                if ("ccc".equals(value)) {
                    value = null;
                }
                return value;
            }
        }, eventConsumer, metrics);
        processor.processRecords(input);

        verify(checkpointer).checkpoint();
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
                .when(checkpointer).checkpoint();
        List<Record> expectedRecords = records(Arrays.asList("aaa", "bbb", "ccc"));
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        BatchProcessor<String> processor = new BatchProcessor<>(MAPPER, event -> true, metrics);
        processor.processRecords(input);

        verify(checkpointer).checkpoint(); //but still counts as processed
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
                .when(checkpointer).checkpoint();
        List<Record> expectedRecords = records(Arrays.asList("aaa", "bbb", "ccc"));
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withRecords(expectedRecords)
                .withCheckpointer(checkpointer);

        BatchProcessor<String> processor = new BatchProcessor<>(MAPPER, event -> true, metrics);
        processor.processRecords(input);
        verify(checkpointer).checkpoint(); //but still counts as processed
        assertThat(metricRegistry.meter("foo-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.meter("foo-decode-success").getCount()).isEqualTo(3);
        assertThat(metricRegistry.meter("foo-decode-failure").getCount()).isEqualTo(0);
        assertThat(metricRegistry.timer("foo-checkpoint").getCount()).isEqualTo(1);
        assertThat(metricRegistry.meter("foo-checkpoint-failure").getCount()).isEqualTo(1);
    }

    @Test
    public void startupShutdownMetrics() throws Exception {
        BatchProcessor<String> processor = new BatchProcessor<>(MAPPER, event -> true, metrics);
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
