package io.codemonastery.dropwizard.kinesis.circle;

import com.amazonaws.services.kinesis.model.Record;
import io.codemonastery.dropwizard.kinesis.KinesisRecordProcessor;
import io.codemonastery.dropwizard.kinesis.SendService;
import io.codemonastery.dropwizard.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class CircleService extends Application<CircleConfiguration> {

    public static void main(String[] args) throws Exception {
        new CircleService().run(args);
    }

    public CircleService() {
    }

    @Override
    public String getName() {
        return "circle-service";
    }

    @Override
    public void initialize(Bootstrap<CircleConfiguration> bootstrap) {
        // Enable variable substitution with environment variables
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false)
                )
        );
    }

    @Override
    public void run(CircleConfiguration configuration,
                    Environment environment) throws InterruptedException {

        final SendService kinesisSendService = configuration
                .getSendService()
                .buildKinesisSendService(environment, "circle-send-service");


        final CircleResource circleResource = new CircleResource(kinesisSendService);

        configuration.getConsumerProcessFactory()
                .buildKinesisConsumerService(configuration.getName(), environment, new CircleProcessorFactory(){

                    final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

                    @Override
                    public IRecordProcessor createProcessor() {
                        return new KinesisRecordProcessor() {
                            @Override
                            public void processSingleRecord(Record record) {
                                try {
                                    final String message = decoder.decode(record.getData()).toString();
                                    circleResource.messageSeen(message);
                                    LOG.info(String.format("consumed a record: %s, %s ",record.getApproximateArrivalTimestamp(), message));
                                } catch (CharacterCodingException e) {
                                    LOG.error("Exception decoding or processing message", e);
                                }
                            }
                        };
                    }
                });


        environment.jersey().register(circleResource);
    }
}