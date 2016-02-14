package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.proxies.KinesisProxy;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import java.nio.charset.Charset;
import java.util.List;

public class KinesisConsumerFactory extends KinesisStreamConfiguration {

    public void build(AmazonKinesis kinesisClient, String name){
        final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
                null,
                getStreamName(),
                null,
                null
        );

        final Worker worker = new Worker.Builder()
                .recordProcessorFactory(() -> {
                    return new IRecordProcessor() {
                        @Override
                        public void initialize(String s) {

                        }

                        @Override
                        public void processRecords(List<Record> list, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {
                            for (Record record : list) {
                                System.out.println(new String(record.getData().array(), Charset.defaultCharset()));
                                try {
                                    iRecordProcessorCheckpointer.checkpoint();
                                } catch (InvalidStateException e) {
                                    e.printStackTrace();
                                } catch (ShutdownException e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        @Override
                        public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

                        }
                    };
                })
                .kinesisClient(kinesisClient)
//                .dynamoDBClient()
                .cloudWatchClient(null)
                .build();

        worker.shutdown();
    }
}
