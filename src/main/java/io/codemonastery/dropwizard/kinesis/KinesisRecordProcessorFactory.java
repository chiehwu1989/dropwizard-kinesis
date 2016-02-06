package io.codemonastery.dropwizard.kinesis;

import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * Used to create new record processors.
 */
public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {
    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisRecordProcessor();
    }
}
