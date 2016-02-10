package io.codemonastery.dropwizard.kinesis.example;


import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * Used to create new record processors.
 */
public class KinesisRecordProcessorFactoryExample implements IRecordProcessorFactory {
    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisRecordProcessorExample();
    }


}
