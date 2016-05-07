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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

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
        kinesis = new KinesisFactory().region(Regions.US_WEST_2).build(null, lifecycle, new DefaultAWSCredentialsProviderChain(), "kinesis");
        dynamodb = new DynamoDbFactory().region(Regions.US_WEST_2).build(null, lifecycle, new DefaultAWSCredentialsProviderChain(), "dynamodb");
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
    public void tryHardToExceedRateLimit() throws Throwable {
        final int numRecords = 10000;
        final Set<String> actualRecords = new HashSet<>();
        final String prefix = UUID.randomUUID().toString();
        BufferedProducer<String> producer = producerFactory.build(null, null, lifecycle, kinesis, "test-producer");
        consumerFactory.consumer(() -> event -> {
            if(event.startsWith(prefix)){
                synchronized (actualRecords){
                    actualRecords.add(event);
                }
            }
            return true;
        }).build(null, null, lifecycle, kinesis, dynamodb, "test-consumer");

        final Set<String> expectedRecords = new HashSet<>();
        for (int i = 0; i < numRecords; i++) {
            String record = prefix + "_" + Integer.toString(i);
            expectedRecords.add(record);
            producer.send(record);
        }

        Assertions.retry(80, Duration.seconds(2), () -> {
            assertThat(actualRecords.size()).isEqualTo(expectedRecords.size());
            synchronized (actualRecords) {
                assertThat(actualRecords).isEqualTo(expectedRecords);
            }
        });
    }
}
