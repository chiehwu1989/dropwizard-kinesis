package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleWorker;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import io.codemonastery.dropwizard.kinesis.*;
import io.codemonastery.dropwizard.kinesis.consumer.ConsumerFactory;
import io.codemonastery.dropwizard.kinesis.consumer.EventConsumer;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.util.Duration;
import org.eclipse.jetty.util.component.LifeCycle;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@Ignore
public class BufferedProducerIT {

    @Rule
    public final TestRule TIMEOUT = new Timeout(400, TimeUnit.SECONDS);

    private static final EventObjectMapper<String> ENCODER = new EventObjectMapper<>(Jackson.newObjectMapper(), String.class);

    private static final String STREAM_NAME = "BufferedProducerIT";

    private AmazonKinesis kinesis;
    private AmazonDynamoDB dynamodb;

    private LifecycleEnvironment lifecycle;
    private BufferedProducerFactory<String> producerFactory;
    private ConsumerFactory<String> consumerFactory;

    @Before
    public void setUp() throws Exception {
        lifecycle = new LifecycleEnvironment();
        kinesis = new KinesisFactory().region(Regions.US_WEST_2).build(null, null, lifecycle, new DefaultAWSCredentialsProviderChain(), "kinesis");
        dynamodb = new DynamoDbFactory().region(Regions.US_WEST_2).build(null, null, lifecycle, new DefaultAWSCredentialsProviderChain(), "dynamodb");
        producerFactory = new BufferedProducerFactory<String>()
                .create(new StreamCreateConfiguration())
                .encoder(ENCODER)
                .streamName(STREAM_NAME);

        consumerFactory = new ConsumerFactory<String>()
                .applicationName(STREAM_NAME + "-test-consumer")
                .create(new StreamCreateConfiguration())
                .decoder(ENCODER::decode)
                .streamName(STREAM_NAME);


    }

    @After
    public void tearDown() throws Exception {
        for (LifeCycle lifeCycle : lifecycle.getManagedObjects()) {
            lifeCycle.stop();
        }
//        try {
//            kinesis.deleteStream(STREAM_NAME);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        try {
//            dynamodb.deleteTable(consumerFactory.getApplicationName());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    @Test
    public void noSendNoPutRecords() throws Throwable {
        final int numRecords = 10000;
        final AtomicInteger count = new AtomicInteger(0);
        final String prefix = UUID.randomUUID().toString();
        BufferedProducer<String> producer = producerFactory.build(null, null, lifecycle, kinesis, "test-producer");
        consumerFactory.consumer(() -> event -> {
            if(event.startsWith(prefix)){
                count.incrementAndGet();
            }

            return true;
        }).build(null, null, lifecycle, kinesis, dynamodb, "test-consumer");

        Thread.sleep(20000);

        for (int i = 0; i < numRecords; i++) {
            producer.send(prefix + "_" + Integer.toString(i));
        }

        Assertions.retry(80, Duration.seconds(2), () -> assertThat(count.get()).isEqualTo(numRecords));
    }
}