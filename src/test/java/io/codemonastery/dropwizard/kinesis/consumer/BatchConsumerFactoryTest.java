package io.codemonastery.dropwizard.kinesis.consumer;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.util.StringInputStream;
import com.amazonaws.util.json.Jackson;
import io.codemonastery.dropwizard.kinesis.ConfigurationFactories;
import io.codemonastery.dropwizard.kinesis.Environments;
import io.codemonastery.dropwizard.kinesis.Event;
import io.codemonastery.dropwizard.kinesis.EventDecoder;
import io.codemonastery.dropwizard.kinesis.EventObjectMapper;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.MockitoAnnotations.initMocks;

public class BatchConsumerFactoryTest {

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @NotNull
        @Valid
        public BatchConsumerFactory<String> consumer;

    }

    @Mock
    private AmazonKinesis kinesis;

    @Mock
    private AmazonDynamoDB dynamoDb;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void inferClassUsingAnonymousClass() throws Exception {
        EventObjectMapper<Event> eventObjectMapper = new BatchConsumerFactory<Event>(){}.inferDecoder(Jackson.getObjectMapper());
        assertThat(eventObjectMapper).isNotNull();
    }

    @Test
    public void minimalConfig() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("consumer:\n  streamName: xyz"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.consumer.getStreamName()).isEqualTo("xyz");

        Environments.run("app", env -> {
            configuration.consumer.decoder(b -> "")
                    .build(env, kinesis, dynamoDb, "aaa"); // build so we can that check status of inferred
            assertThat(configuration.consumer.getConsumer()).isNotNull();
            assertThat(configuration.consumer.getConsumer().get().consume(Collections.singletonList("aaa"))).isTrue();
        });
    }

    @Test
    public void canSetSomeKclConfigs() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("consumer:\n  streamName: xyz\n  initialPositionInStream: TRIM_HORIZON"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.consumer.getStreamName()).isEqualTo("xyz");
        assertThat(configuration.consumer.getInitialPositionInStream()).isEqualTo(InitialPositionInStream.TRIM_HORIZON);
    }

    @Test
    public void allTheThings() throws Exception {
        String streamName = "xyz";
        EventDecoder<String> decoder = new EventDecoder<String>() {
            @Nullable
            @Override
            public String decode(ByteBuffer bytes) throws Exception {
                return new String(bytes.array());
            }
        };
        Supplier<BatchConsumer<String>> batchConsumerSupplier = () -> e -> true;

        BatchConsumerFactory factory = new BatchConsumerFactory<String>()
                .streamName(streamName)
                .decoder(decoder)
                .consumer(batchConsumerSupplier);

        assertThat(factory.getStreamName()).isEqualTo(streamName);
        assertThat(factory.getDecoder()).isSameAs(decoder);
        assertThat(factory.getConsumer()).isSameAs(batchConsumerSupplier);

        Environments.run("app", env->{
            assertThat(env.lifecycle().getManagedObjects().size()).isEqualTo(1);
            factory.build(env,
                    kinesis,
                    dynamoDb,
                    "foo");

            assertThat(env.metrics().getNames()).contains("foo-success");
            assertThat(env.lifecycle().getManagedObjects().size()).isEqualTo(4);
            assertThat(env.healthChecks().getNames()).contains("foo");
        });
    }

    @Test(expected = NullPointerException.class)
    public void buildCannotInferDecoder() throws Exception {
        Environments.run("app", env-> new BatchConsumerFactory<String>().streamName("xyz").build(env, kinesis, dynamoDb, "foo"));
    }

    @Test
    public void buildCanInferDecoder() throws Exception {
        Environments.run("app", env->{
            BatchConsumerFactory<String> factory = new BatchConsumerFactory<String>() {}.streamName("xyz");
            assertThat(factory.getDecoder()).isNull();
            factory.build(env, kinesis, dynamoDb, "foo");
            assertThat(factory.getDecoder()).isNotNull();
        });
    }

    @Test
    public void canInheritDecoder() throws Exception {
        Environments.run("app", env->{
            BatchConsumerFactory<String> parentFactory = new BatchConsumerFactory<String>() {}.streamName("xyz");
            BatchConsumerFactory<String> factory = new BatchConsumerFactory<String>().streamName("xyz").inheritDecoder(parentFactory);

            assertThat(factory.getDecoder()).isNull();
            factory.build(env, kinesis, dynamoDb, "foo");
            assertThat(factory.getDecoder()).isNotNull();
        });
    }

    @Test(expected = NullPointerException.class)
    public void canBuildWithoutEnvironmentAndDecoder() throws Exception {
        BatchConsumerFactory<String> factory = new BatchConsumerFactory<String>(){}.streamName("xyz");
        factory.build(null, kinesis, dynamoDb, "foo");
    }


}
