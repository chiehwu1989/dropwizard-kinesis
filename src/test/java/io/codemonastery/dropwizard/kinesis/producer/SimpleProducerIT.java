package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import io.codemonastery.dropwizard.kinesis.*;
import io.codemonastery.dropwizard.kinesis.consumer.ConsumerFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.util.Duration;
import org.eclipse.jetty.util.component.LifeCycle;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public class SimpleProducerIT {

    @Rule
    public final TestRule TIMEOUT = new Timeout(400, TimeUnit.SECONDS);

    private static final EventObjectMapper<String> ENCODER = new EventObjectMapper<>(Jackson.newObjectMapper(), String.class);

    private static final String STREAM_NAME = "SimpleProducerIT";

    private AmazonKinesis kinesis;
    private AmazonDynamoDB dynamodb;

    private LifecycleEnvironment lifecycle;
    private SimpleProducerFactory<String> producerFactory;
    private ConsumerFactory<String> consumerFactory;

    @Before
    public void setUp() throws Exception {
        lifecycle = new LifecycleEnvironment();
        kinesis = new KinesisFactory().region(Regions.US_WEST_2).build(null, null, lifecycle, new DefaultAWSCredentialsProviderChain(), "kinesis");
        dynamodb = new DynamoDbFactory().region(Regions.US_WEST_2).build(null, null, lifecycle, new DefaultAWSCredentialsProviderChain(), "dynamodb");
        producerFactory = new SimpleProducerFactory<String>()
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
    public void tryHardToExceedRateLimit() throws Throwable {
        final int numRecords = 10000;
        final AtomicInteger count = new AtomicInteger(0);
        final String prefix = UUID.randomUUID().toString();
        SimpleProducer<String> producer = producerFactory.build(null, null, lifecycle, kinesis, "test-producer");
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
