package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import io.codemonastery.dropwizard.kinesis.rule.KinesisClientRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@Ignore
public class StreamConfigurationIT {

    @Rule
    public final KinesisClientRule CLIENT_RULE = new KinesisClientRule();

    @Test
    public void noDefaultShardCountHenceDoNotCreate() throws Exception {
        String streamName = CLIENT_RULE.streamName();

        StreamConfiguration configuration = new StreamConfiguration();
        configuration.setStreamName(streamName);
        configuration.setupStream(CLIENT_RULE.getClient());

        try{
            //noinspection unused
            DescribeStreamResult describeStreamResult = CLIENT_RULE.getClient().describeStream(streamName);
            fail("was supposed to except sinc stream not created");
        }catch (ResourceNotFoundException e){
            //perfect
        }
    }

    @Test
     public void createStreamAsNeededWaitForActive() throws Exception {
        String streamName = CLIENT_RULE.streamName();

        StreamConfiguration configuration = new StreamConfiguration();
        configuration.setStreamName(streamName);

        configuration.setupStream(CLIENT_RULE.getClient());

        DescribeStreamResult result = CLIENT_RULE.getClient().describeStream(streamName);
        assertThat(result).isNotNull();
        assertThat(result.getStreamDescription()).isNotNull();
        assertThat(result.getStreamDescription().getStreamName()).isEqualTo(streamName);
        assertThat(result.getStreamDescription().getStreamStatus()).isEqualTo("ACTIVE");
    }

    @Test
    public void streamAlreadyCreatedWaitForActive() throws Exception {
        String streamName = CLIENT_RULE.streamName();

        CLIENT_RULE.getClient().createStream(streamName, 1);

        StreamConfiguration configuration = new StreamConfiguration();
        configuration.setStreamName(streamName);

        configuration.setupStream(CLIENT_RULE.getClient());

        DescribeStreamResult result = CLIENT_RULE.getClient().describeStream(streamName);
        assertThat(result).isNotNull();
        assertThat(result.getStreamDescription()).isNotNull();
        assertThat(result.getStreamDescription().getStreamName()).isEqualTo(streamName);
        assertThat(result.getStreamDescription().getStreamStatus()).isEqualTo("ACTIVE");
    }
}
