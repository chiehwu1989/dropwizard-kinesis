package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import io.codemonastery.dropwizard.kinesis.EventDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BatchProcessor<E> implements IRecordProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(BatchProcessor.class);

    private final EventDecoder<E> decoder;
    private final BatchConsumer<E> processor;
    private final BatchProcessorMetrics metrics;


    public BatchProcessor(EventDecoder<E> decoder, BatchConsumer<E> processor, BatchProcessorMetrics metrics) {
        this.decoder = decoder;
        this.processor = processor;
        this.metrics = metrics;
    }


    @Override
    public void initialize(InitializationInput initializationInput) {
        metrics.processorStarted();
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        boolean processed = false;
        List<E> batch = decodeBatch(processRecordsInput.getRecords());

        if(batch != null){
            try {
                processed = processor.consume(batch);
            } catch (Exception e) {
                metrics.unhandledException();
                //processor did not catch exception, we have to stop here
                LOG.error("Unhandled exception processing batch: " + batch, e);
            }
            if(processed){
                for (int i = 0; i < batch.size(); i++) {
                    metrics.processSuccess();
                }
                try (AutoCloseable ignore = metrics.checkpointTime()) {
                    processRecordsInput.getCheckpointer().checkpoint();
                } catch (ShutdownException e) {
                    metrics.checkpointFailed();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Abandoning checkpoint because processor was shutdown");
                    }
                } catch (Exception e) {
                    metrics.checkpointFailed();
                    LOG.error("Could not checkpoint because of unexpected exception", e);
                }
            }
        }

        if(!processed) {
            for (int i = 0; i < processRecordsInput.getRecords().size(); i++) {
                metrics.processFailure();
            }
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        metrics.processorShutdown();
    }

    /*
     * @returns batch if there were no decoding errors, null otherwise.
     */
    private List<E> decodeBatch(List<Record> records) {
        List<E> batch = new ArrayList<>();
        for (Record record : records) {
            try {
                E event = decoder.decode(record.getData());
                metrics.decoded();
                if (event == null) {
                    LOG.warn("Decoder returned null, omitting from batch to be consumed");
                } else {
                    batch.add(event);
                }
            } catch (Exception e) {
                metrics.decodeFailure();
                LOG.error("Unexpected exception decoding event", e);
                batch = null;
                break;
            }
        }
        return batch;
    }
}
