package io.codemonastery.dropwizard.kinesis.circle;

import com.google.common.collect.ImmutableList;
import io.codemonastery.dropwizard.kinesis.KinesisProducerFactory;
import io.codemonastery.dropwizard.kinesis.rule.KinesisClientRule;
import io.dropwizard.testing.junit.DropwizardAppRule;
import io.dropwizard.util.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import java.util.List;

public class CircleApplicationIT {

    @ClassRule
    public static final TestRule ORDERED_CLASS_RULE;

    private static final KinesisClientRule CLIENT_RULE = new KinesisClientRule();
    private static final DropwizardAppRule<CircleConfiguration> APP_RULE;


    static {
        CircleConfiguration circleConfiguration = new CircleConfiguration();
        {
            KinesisProducerFactory<String> producer = circleConfiguration.getProducer();
            producer.setStreamName(CLIENT_RULE.streamName());
            producer.setDefaultShardCount(1);
            producer.setFlushPeriod(Duration.milliseconds(500));
        }
        circleConfiguration.getKinesis().setRegion(KinesisClientRule.TEST_REGIONS);
        APP_RULE = new DropwizardAppRule<>(CircleApplication.class, circleConfiguration);

        ORDERED_CLASS_RULE = RuleChain.outerRule(CLIENT_RULE).around(APP_RULE);
    }

    private Client client;


    @Before
    public void setUp() {
        client = ClientBuilder.newClient();
    }

    @After
    public void tearDownAfter() throws Exception {
        client.close();
    }

    @Test
    public void someRecords() throws Exception {
        final List<String> expected = ImmutableList.of("hello seinfeld", "hello newman", "george!");
        {
            final String[] sendMe = expected.toArray(new String[expected.size()]);
            client.target("http://localhost:" + APP_RULE.getLocalPort() + "/")
                    .request()
                    .post(Entity.entity(sendMe, MediaType.APPLICATION_JSON_TYPE));
        }
//        {
//            final List<String> actual = new ArrayList<>();
//
//            final int numRetries = 100;
//            for (int i = 0; i < numRetries; i++) {
//                try {
//                    final String[] seen = client.target("http://localhost:" + RULE.getLocalPort() + "/circle/")
//                            .request()
//                            .get(String[].class);
//                    Collections.addAll(actual, seen);
//                    assertEquals(expected, actual);
//                } catch (AssertionError e) {
//                    if (i == numRetries - 1) {
//                        throw e;
//                    }
//                }
//                Thread.sleep(500);
//            }
//        }
        for(int i = 0; i < 30; i++){
            Thread.sleep(1000);
        }
        System.out.println("DONE");
    }

}
