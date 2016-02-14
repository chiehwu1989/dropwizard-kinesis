package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import io.codemonastery.dropwizard.kinesis.rule.KinesisClientRule;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class KinesisStreamConfigurationIT {

    private static final Logger LOG = LoggerFactory.getLogger(KinesisStreamConfigurationIT.class);

    private final String streamName = this.getClass().getSimpleName() + UUID.randomUUID().toString().substring(0, 10);

    @ClassRule
    public static final KinesisClientRule CLIENT_RULE = new KinesisClientRule();

    @After
    public void tearDown() throws Exception {
        try{
            CLIENT_RULE.getClient().deleteStream(streamName);
        }catch (ResourceNotFoundException re){
            //ignore
        }catch (Exception e){
            LOG.error("Could not cleanup stream", e);
        }
    }

    @Test
     public void createStreamAsNeeded() throws Exception {
        KinesisStreamConfiguration configuration = new KinesisStreamConfiguration();
        configuration.setStreamName(streamName);

        configuration.setupStream(CLIENT_RULE.getClient());

        DescribeStreamResult result = CLIENT_RULE.getClient().describeStream(streamName);
        assertThat(result).isNotNull();
        assertThat(result.getStreamDescription()).isNotNull();
        assertThat(result.getStreamDescription().getStreamName()).isEqualTo(streamName);
    }

    @Test
    public void streamAlreadyCreated() throws Exception {
        CLIENT_RULE.getClient().createStream(streamName, 1);
        KinesisStreamConfiguration configuration = new KinesisStreamConfiguration();
        configuration.setStreamName(streamName);

        configuration.setupStream(CLIENT_RULE.getClient());

        DescribeStreamResult result = CLIENT_RULE.getClient().describeStream(streamName);
        assertThat(result).isNotNull();
        assertThat(result.getStreamDescription()).isNotNull();
        assertThat(result.getStreamDescription().getStreamName()).isEqualTo(streamName);
    }
}