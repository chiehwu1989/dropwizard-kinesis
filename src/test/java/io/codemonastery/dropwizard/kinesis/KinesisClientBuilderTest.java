package io.codemonastery.dropwizard.kinesis;

import org.junit.Test;

public class KinesisClientBuilderTest {

    @Test
    public void nullEnvironment() throws Exception {
        new KinesisClientBuilder().build(null, new NoCredentialsProvider(), "test-CLIENT_RULE");
    }
}
