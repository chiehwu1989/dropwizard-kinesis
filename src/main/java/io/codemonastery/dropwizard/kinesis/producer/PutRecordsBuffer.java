package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

class PutRecordsBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(PutRecordsBuffer.class);

    private final ArrayDeque<PutRecordsRequestEntry> queue;
    private final int maxBufferByteSize;

    private int bufferByteSize;
    private int maxBufferRecordSize;

    public PutRecordsBuffer(int maxBufferRecordSize){
        this(maxBufferRecordSize, Producer.MAX_REQUEST_SIZE);
    }

    public PutRecordsBuffer(int maxBufferRecordSize, int maxBufferByteSize) {
        this.maxBufferByteSize = maxBufferByteSize;
        this.maxBufferRecordSize = maxBufferRecordSize;
        queue = new ArrayDeque<>(this.maxBufferRecordSize);
    }

    public synchronized List<List<PutRecordsRequestEntry>> addAll(List<PutRecordsRequestEntry> es){
        List<List<PutRecordsRequestEntry>> submitMes = new ArrayList<>();
        for (PutRecordsRequestEntry e : es) {
            List<PutRecordsRequestEntry> submitMe = add(e);
            if(submitMe != null){
                submitMes.add(submitMe);
            }
        }
        return submitMes;
    }

    /**
     * Adds the entry to this buffer, and returns list entries which need to be sent, null if buffer is not null
     * @param e entry to add
     * @return null if not ready to send entries, a list otherwize
     */
    public synchronized List<PutRecordsRequestEntry> add(PutRecordsRequestEntry e){
        List<PutRecordsRequestEntry> submitMe = null;

        if(e.getData().limit() > maxBufferByteSize){
            LOG.error("Encountered extreme degenerate case, record was too large to fit in buffer, should have never encountered this!");
        }else{
            //if adding e is too many bytes or too many records
            if(bufferByteSize + e.getData().limit() > maxBufferByteSize ||
                    queue.size() == maxBufferRecordSize){
                submitMe = new ArrayList<>(queue);
                queue.clear();
                bufferByteSize = 0;
            }

            queue.add(e);
            bufferByteSize += e.getData().limit();
        }

        return submitMe;
    }

    /**
     * Returns list of entries to send, removes them from buffer
     * @return a possibly empty list, never null
     */
    public synchronized List<PutRecordsRequestEntry> drain(){
        List<PutRecordsRequestEntry> submitMe = new ArrayList<>(queue);
        queue.clear();
        bufferByteSize = 0;
        return submitMe;
    }
}
