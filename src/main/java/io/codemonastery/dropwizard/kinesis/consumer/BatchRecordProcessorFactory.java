package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventDecoder;

import java.util.function.Supplier;

public class BatchRecordProcessorFactory<E> implements IRecordProcessorFactory {

    private final EventDecoder<E> decoder;
    private final Supplier<BatchConsumer<E>> eventConsumerFactory;
    private final BatchProcessorMetrics metrics;

    public BatchRecordProcessorFactory(EventDecoder<E> decoder,
                                       Supplier<BatchConsumer<E>> eventConsumerFactory,
                                       BatchProcessorMetrics metrics) {
        Preconditions.checkNotNull(decoder, "decoder cannot be null");
        Preconditions.checkNotNull(eventConsumerFactory, "eventConsumerFactory cannot be null");
        Preconditions.checkNotNull(metrics, "metrics cannot be null");
        this.decoder = decoder;
        this.eventConsumerFactory = eventConsumerFactory;
        this.metrics = metrics;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new BatchProcessor<>(decoder, eventConsumerFactory.get(), metrics);
    }
}
