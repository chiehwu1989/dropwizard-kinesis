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

public final class RecordProcessor<E> implements IRecordProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(RecordProcessor.class);

    private final EventDecoder<E> decoder;
    private final EventConsumer<E> processor;
    private final RecordProcessorMetrics metrics;

    private String shardId = null;

    public RecordProcessor(EventDecoder<E> decoder,
                           EventConsumer<E> eventConsumer,
                           RecordProcessorMetrics metrics) {
        Preconditions.checkNotNull(decoder, "decoder cannot be null");
        Preconditions.checkNotNull(eventConsumer, "eventConsumer cannot be null");
        Preconditions.checkNotNull(metrics, "metrics cannot be null");
        this.decoder = decoder;
        this.processor = eventConsumer;
        this.metrics = metrics;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        if(initializationInput != null){
            shardId = initializationInput.getShardId();
        }
        metrics.processorStarted();
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        if(processRecordsInput.getMillisBehindLatest() != null){
            metrics.millisBehindLatest(shardId, processRecordsInput.getMillisBehindLatest());
        }
        Record lastRecordProcessed = null;
        for (Record record : processRecordsInput.getRecords()) {
            E event;

            try{
                event = decoder.decode(record.getData());
                metrics.decoded();
            }catch (Exception e){
                //unhandled exception, this record does not count as processed
                metrics.decodeFailure();
                LOG.error("Unexpected exception decoding event", e);
                break;
            }

            if(event == null){
                //since decoder returned null without exception we should treat this record as processed
                lastRecordProcessed = record;
            }else {
                boolean processed = false;
                try(AutoCloseable ignored = metrics.processTime()) {
                    processed = processor.consume(event);
                } catch (Exception e) {
                    metrics.unhandledException();
                    //processor did not catch exception, we have to stop here
                    LOG.error("Unhandled exception processing event" + event, e);
                }
                if(processed){
                    metrics.processSuccess();
                    lastRecordProcessed = record;
                }else{
                    metrics.processFailure();
                    break;
                }
            }
        }

        if(lastRecordProcessed != null){
            try(AutoCloseable ignore = metrics.checkpointTime()) {
                processRecordsInput.getCheckpointer().checkpoint(lastRecordProcessed);
            } catch (ShutdownException e) {
                metrics.checkpointFailed();
                if(LOG.isDebugEnabled()){
                    LOG.debug("Abandoning checkpoint because processor was shutdown");
                }
            } catch (Exception e){
                metrics.checkpointFailed();
                LOG.error("Could not checkpoint because of unexpected exception", e);
            }
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        shardId = "UNKNOWN";
        metrics.processorShutdown(shardId);
    }
}
