package io.codemonastery.dropwizard.kinesis.circle;

import com.google.common.collect.ImmutableList;
import io.codemonastery.dropwizard.kinesis.rule.KinesisClientRule;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public class CircleApplicationIT {

    @ClassRule
    public static final TestRule ORDERED_CLASS_RULE;

    private static final KinesisClientRule CLIENT_RULE = new KinesisClientRule();
    private static final DropwizardAppRule<CircleConfiguration> APP_RULE;


    static {

        APP_RULE = new DropwizardAppRule<>(CircleApplication.class, new File("./src/test/resources/config.circle.yml").getPath());

        ORDERED_CLASS_RULE = RuleChain.outerRule(CLIENT_RULE).around(APP_RULE);
    }

    private Client client;
    private WebTarget circleTarget;

    @Before
    public void setUp() {
        client = ClientBuilder.newClient();
        circleTarget = client.target("http://localhost:" + APP_RULE.getLocalPort() + "/");
    }

    @After
    public void tearDownAfter() throws Exception {
        circleTarget.request().delete();
        client.close();
    }

    @Test
    public void someRecords() throws Exception {


        final List<String> expected = ImmutableList.of("hello seinfeld", "hello newman", "george!");
        {
            final String[] sendMe = expected.toArray(new String[expected.size()]);
            circleTarget
                    .request()
                    .post(Entity.entity(sendMe, MediaType.APPLICATION_JSON_TYPE));
        }
        {
            final int numRetries = 25;
            for (int i = 0; i < numRetries; i++) {
                try {
                    final String[] actual = circleTarget
                            .request()
                            .get(String[].class);
                    assertThat(Arrays.asList(actual)).isEqualTo(expected);
                } catch (AssertionError e) {
                    if (i == numRetries - 1) {
                        throw e;
                    }
                }
                Thread.sleep(2000);
            }
        }
    }

}
