package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.model.Record;
import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

/**
 * Used to create new record processors.
 */
public interface KinesisRecordProcessorFactory extends IRecordProcessorFactory {
    Logger LOG = LoggerFactory.getLogger(KinesisRecordProcessorFactory.class);
    CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();


    /**
     * {@inheritDoc}
     */
    default IRecordProcessor createProcessor() {
        return new KinesisRecordProcessor() {
            @Override
            public void processSingleRecord(Record record) {
                // TODO Add your own record processing logic here

                String data = null;
                try {
                    // For this app, we interpret the payload as UTF-8 chars.
                    data = decoder.decode(record.getData()).toString();
                    // Assume this record came from AmazonKinesisSample and log its age.
                    long recordCreateTime = new Long(data.substring("testData-".length()));
                    long ageOfRecordInMillis = System.currentTimeMillis() - recordCreateTime;

                    LOG.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + data + ", Created "
                            + ageOfRecordInMillis + " milliseconds ago.");
                } catch (NumberFormatException e) {
                    LOG.info("Record does not match sample record format. Ignoring record with data; " + data);
                } catch (CharacterCodingException e) {
                    LOG.error("Malformed data: " + data, e);
                }
            }
        };
    }



}
