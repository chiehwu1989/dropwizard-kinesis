package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.google.common.base.Preconditions;
import io.codemonastery.dropwizard.kinesis.EventDecoder;

import java.util.function.Supplier;

public class RecordProcessorFactory<E> implements IRecordProcessorFactory {

    private final EventDecoder<E> decoder;
    private final Supplier<EventConsumer<E>> eventConsumerFactory;
    private final RecordProcessorMetrics metrics;

    public RecordProcessorFactory(EventDecoder<E> decoder,
                                  Supplier<EventConsumer<E>> eventConsumerFactory,
                                  RecordProcessorMetrics metrics) {
        Preconditions.checkNotNull(decoder, "decoder cannot be null");
        Preconditions.checkNotNull(eventConsumerFactory, "eventConsumerFactory cannot be null");
        Preconditions.checkNotNull(metrics, "metrics cannot be null");
        this.decoder = decoder;
        this.eventConsumerFactory = eventConsumerFactory;
        this.metrics = metrics;
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new RecordProcessor<>(decoder, eventConsumerFactory.get(), metrics);
    }
}
