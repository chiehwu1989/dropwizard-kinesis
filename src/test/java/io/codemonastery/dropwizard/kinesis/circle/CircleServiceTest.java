package io.codemonastery.dropwizard.kinesis.circle;

import com.google.common.collect.ImmutableList;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CircleServiceTest {

    private final static String CONFIG_PATH = ResourceHelpers.resourceFilePath("config.test.yml");

    @ClassRule
    public static final DropwizardAppRule<CircleConfiguration> RULE = new DropwizardAppRule<>(
            CircleService.class,
            CONFIG_PATH);


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
            client.target("http://localhost:" + RULE.getLocalPort() + "/circle/")
                    .request()
                    .post(Entity.entity(sendMe, MediaType.APPLICATION_JSON_TYPE));
        }
        {
            final List<String> actual = new ArrayList<>();

            final int numRetries = 100;
            for (int i = 0; i < numRetries; i++) {
                try {
                    final String[] seen = client.target("http://localhost:" + RULE.getLocalPort() + "/circle/")
                            .request()
                            .get(String[].class);
                    Collections.addAll(actual, seen);
                    assertEquals(expected, actual);
                } catch (AssertionError e) {
                    if (i == numRetries - 1) {
                        throw e;
                    }
                }
                Thread.sleep(500);
            }
        }
    }


}
