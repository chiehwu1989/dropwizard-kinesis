package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.SimpleWorker;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.SplitShardResult;
import com.codahale.metrics.MetricRegistry;
import io.codemonastery.dropwizard.kinesis.DynamoDbFactory;
import io.codemonastery.dropwizard.kinesis.EventDecoder;
import io.codemonastery.dropwizard.kinesis.EventEncoder;
import io.codemonastery.dropwizard.kinesis.KinesisFactory;
import io.codemonastery.dropwizard.kinesis.producer.Producer;
import io.codemonastery.dropwizard.kinesis.producer.SimpleProducerFactory;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public class ShardSplitIT {

    private static final Logger LOG = LoggerFactory.getLogger(ShardSplitIT.class);

    public static final String STREAM = "ShardSplitIT";
    private DefaultAWSCredentialsProviderChain credentialsProvider = new DefaultAWSCredentialsProviderChain();
    private AmazonKinesis kinesis;
    private AmazonDynamoDB dynamoDB;
    private Producer<Long> producer;
    private ConsumerFactory<Long> consumerFactory;

    @Before
    public void setUp() throws Exception {
        kinesis = new KinesisFactory()
                .region(Regions.US_WEST_2)
                .build(null, credentialsProvider, "ShardSplitIT_kinesis");

        dynamoDB = new DynamoDbFactory()
                .region(Regions.US_WEST_2)
                .build(null, credentialsProvider, "ShardSplitIT_dynamoDB");


        producer = new SimpleProducerFactory<Long>()
                .streamName(STREAM)
                .encoder(new EventEncoder<Long>() {
                    @Nullable
                    @Override
                    public byte[] encode(Long event) throws Exception {
                        return event.toString().getBytes();
                    }
                })
                .build(null, kinesis, "ShardSplitIT_producer");

        consumerFactory = new ConsumerFactory<Long>()
                .streamName(STREAM)
                .decoder(new EventDecoder<Long>() {
                    @Nullable
                    @Override
                    public Long decode(ByteBuffer bytes) throws Exception {
                        return Long.parseLong(new String(bytes.array()));
                    }
                });

    }

    @Test
    public void testName() throws Exception {
        blockingDeleteStream();
        blockingCreate(1);

        Thread.sleep(5000);
        ProducerThread producerThread = new ProducerThread(producer);
        Thread consumerThread = null;
        try {
            producerThread.start();
            Thread.sleep(1000);

            LOG.info("Creating consumer...");
            MetricRegistry metrics = new MetricRegistry();
            final AtomicReference<Date> latestEvent = new AtomicReference<>(null);
            SimpleWorker simpleWorker = consumerFactory.consumer(() -> event -> {
                latestEvent.set(new Date());
                return true;
            }).build(metrics, null, null, kinesis, dynamoDB, STREAM);

            consumerThread = new Thread(simpleWorker::run);
            consumerThread.start();

            LOG.info("Done creating consumer.");

            LOG.info("Waiting for first record...");
            while(latestEvent.get() == null){
                Thread.sleep(5000);
            }
            LOG.info("Saw first record.");

            splitFirstShard();
            Date justAfterSplit = latestEvent.get();

            LOG.info("Waiting to see a record after split...");
            int retryCount = 60;
            for (int i = 1; i <= retryCount; i++) {
                Thread.sleep(1000);
                try{
                    assertThat(latestEvent.get()).isAfter(justAfterSplit);
                    break;
                }catch (AssertionError e){
                    if(i == retryCount){
                        throw e;
                    }
                }
            }
            LOG.info("Saw a record after split...");

        } finally {
            producerThread.interrupt();
            if(consumerThread != null){
                consumerThread.interrupt();
            }
        }
    }

    private static class ProducerThread extends Thread {

        private AtomicLong counter = new AtomicLong(1);
        private final Producer<Long> producer;

        public ProducerThread(Producer<Long> producer) {
            this.producer = producer;
        }

        @Override
        public void run() {
            while (this.isAlive() && !this.isInterrupted()) {
                try {
                    long event = counter.getAndIncrement();
                    producer.send(event);
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    // just retry
                }
            }

        }
    }

    private void splitFirstShard() throws InterruptedException {
        List<Shard> originalShards = kinesis.describeStream(STREAM).getStreamDescription().getShards();
        Shard firstShard = originalShards.get(0);
        LOG.info("Splitting shard: " + firstShard.getShardId());
        BigInteger start = new BigInteger(firstShard.getHashKeyRange().getStartingHashKey());
        BigInteger end = new BigInteger(firstShard.getHashKeyRange().getEndingHashKey());
        BigInteger middle = start.add(end).divide(new BigInteger("2"));

        SplitShardResult splitShard = kinesis.splitShard(STREAM, firstShard.getShardId(), middle.toString());

        while (true) {
            Thread.sleep(5000);
            DescribeStreamResult describeStream = kinesis.describeStream(STREAM);
            if (originalShards.size() + 2 == describeStream.getStreamDescription().getShards().size()) {
                break;
            }
        }
        LOG.info("Done splitting.");
    }

    private void blockingCreate(int shardCount) throws InterruptedException {
        LOG.info("Blocking create...");
        kinesis.createStream(STREAM, shardCount);
        Thread.sleep(10000);

        DescribeStreamResult describeStream;
        while (true) {
            describeStream = kinesis.describeStream(STREAM);
            if ("ACTIVE".equalsIgnoreCase(describeStream.getStreamDescription().getStreamStatus())) {
                break;
            }
            Thread.sleep(1000);
        }
        assertThat(describeStream.getStreamDescription().getShards().size()).isEqualTo(shardCount);
    }

    private void blockingDeleteStream() throws InterruptedException {
        LOG.info("Blocking delete...");
        try {
            while (true) {
                DescribeStreamResult describeStream = kinesis.describeStream(STREAM);
                if ("ACTIVE".equalsIgnoreCase(describeStream.getStreamDescription().getStreamStatus())) {
                    kinesis.deleteStream(STREAM);
                }
                Thread.sleep(1000);
            }
        } catch (ResourceNotFoundException e) {
            // this is good.
        }


        try {
            while (true){
                DescribeTableResult describeTable = dynamoDB.describeTable(STREAM);
                if ("ACTIVE".equalsIgnoreCase(describeTable.getTable().getTableStatus())) {
                    dynamoDB.deleteTable(STREAM);
                }
            }
        } catch (com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException e) {
            // this is good.
        }
    }
}
