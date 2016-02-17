package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PutRecordsBufferTest {

    @Test
    public void overflowRecordSize() throws Exception {
        PutRecordsBuffer buffer = new PutRecordsBuffer(10, 1024);
        for (int i = 0; i < 101; i++) {
            List<PutRecordsRequestEntry> submitMe = buffer.add(entry(1));
            if (i > 0 && i % 10 == 0) {
                assertThat(submitMe).isNotNull();
                assertThat(submitMe.size()).isEqualTo(10);
            } else {
                assertThat(submitMe).isNull();
            }
        }
    }

    @Test
    public void overflowByteSize() throws Exception {
        PutRecordsBuffer buffer = new PutRecordsBuffer(10, 1024);
        for (int i = 0; i < 25; i++) {
            List<PutRecordsRequestEntry> submitMe = buffer.add(entry(1024 / 4));
            if (i > 0 && i % 4 == 0) {
                assertThat(submitMe).isNotNull();
                assertThat(submitMe.size()).isEqualTo(4);
            } else {
                assertThat(submitMe).isNull();
            }
        }
    }

    @Test
    public void extremeDegenerateCase() throws Exception {
        PutRecordsBuffer buffer = new PutRecordsBuffer(10, 1024);
        buffer.add(entry(1025));
        assertThat(buffer.drain()).isEmpty();
    }

    @Test
    public void sendAll() throws Exception {
        List<PutRecordsRequestEntry> entries = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            entries.add(entry(1));
        }
        PutRecordsBuffer buffer = new PutRecordsBuffer(10, 1024);
        List<List<PutRecordsRequestEntry>> needToSubmit = buffer.addAll(entries);

        assertThat(needToSubmit.size()).isEqualTo(10)
                .withFailMessage("Should have to submit 10 requests of 10 records each");
        assertThat(buffer.drain().size()).isEqualTo(1)
                .withFailMessage("Should have one record left over");
    }

    private PutRecordsRequestEntry entry(int size) {
        return new PutRecordsRequestEntry().withData(ByteBuffer.wrap(new byte[size]));
    }
}
