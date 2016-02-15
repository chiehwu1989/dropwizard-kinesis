package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordProcessor<E> implements IRecordProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RecordProcessor.class);

    private final EventDecoder<E> decoder;
    private final EventConsumer<E> processor;

    public RecordProcessor(EventDecoder<E> decoder, EventConsumer<E> processor) {
        Preconditions.checkNotNull(decoder, "decoder cannot be null");
        Preconditions.checkNotNull(processor, "processor cannot be null");
        this.decoder = decoder;
        this.processor = processor;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {

    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        Record lastRecordProcessed = null;
        for (Record record : processRecordsInput.getRecords()) {
            E event;

            try{
                event = decoder.decode(record.getData());
            }catch (Exception e){
                //unhandled exception, this record does not count as processed
                LOG.error("Unexpected exception decoding event", e);
                break;
            }

            if(event == null){
                //since decoder returned null without exception we should treat this record as processed
                lastRecordProcessed = record;
            }else {
                boolean processed = false;
                try {
                    processed = processor.process(event);
                } catch (Exception e) {
                    //processor did not catch exception, we have to stop here
                    LOG.error("Unhandled exception processing event" + event, e);
                }
                if(processed){
                    lastRecordProcessed = record;
                }else{
                    break;
                }
            }
        }

        if(lastRecordProcessed != null){
            try {
                processRecordsInput.getCheckpointer().checkpoint(lastRecordProcessed);
            } catch (ShutdownException e) {
                if(LOG.isDebugEnabled()){
                    LOG.debug("Abandoning checkpoint because processor was shutdown");
                }
            } catch (Exception e){
                LOG.error("Could not checkpoint because of unexpected exception", e);
            }
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {

    }
}
