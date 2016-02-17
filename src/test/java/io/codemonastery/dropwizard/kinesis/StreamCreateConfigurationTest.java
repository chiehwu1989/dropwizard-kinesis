package io.codemonastery.dropwizard.kinesis;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceInUseException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.util.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import javax.validation.Valid;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class StreamCreateConfigurationTest {

    private static final String STREAM_NAME = "test-stream";

    private static final DescribeStreamResult ACTIVE = KinesisResults.activeStream(STREAM_NAME);

    private static final DescribeStreamResult NOT_ACTIVE = KinesisResults.creatingStream(STREAM_NAME);

    @Mock
    private AmazonKinesis kinesis;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void emptyConfig() throws Exception {
        ConfigurationFactory<FakeConfiguration> configurationFactory = ConfigurationFactories.make(FakeConfiguration.class);
        FakeConfiguration configuration = configurationFactory.build((s) -> new StringInputStream("create: {}"), "");
        assertThat(configuration).isNotNull();
        assertThat(configuration.create).isNotNull();
        assertThat(configuration.create.getShardCount()).isEqualTo(1);
    }

    @Test
    public void noCreateBecauseAlreadyExists() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenReturn(ACTIVE);
        doThrow(new ResourceInUseException("exists"))
                .when(kinesis)
                .createStream(anyString(), anyInt());

        StreamCreateConfiguration create = new StreamCreateConfiguration()
                .retryPeriod(Duration.milliseconds(100));
        assertThat(callCreateAssertTerminated(create)).isTrue();

        verify(kinesis, times(1)).describeStream(STREAM_NAME);
        verify(kinesis, never()).createStream(anyString(), anyInt());
    }

    @Test
    public void noCreateBecauseNotActive() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenReturn(NOT_ACTIVE)
                .thenReturn(NOT_ACTIVE)
                .thenReturn(ACTIVE);
        doThrow(new ResourceInUseException("exists"))
                .when(kinesis)
                .createStream(anyString(), anyInt());

        StreamCreateConfiguration create = new StreamCreateConfiguration()
                .retryPeriod(Duration.milliseconds(100));
        assertThat(callCreateAssertTerminated(create)).isTrue();

        verify(kinesis, times(3)).describeStream(STREAM_NAME);
        verify(kinesis, never()).createStream(anyString(), anyInt());
    }

    @Test
    public void doesNotExistSoCreate() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenThrow(new ResourceNotFoundException("does not exist yet"))
                .thenReturn(NOT_ACTIVE)
                .thenReturn(NOT_ACTIVE)
                .thenReturn(NOT_ACTIVE)
                .thenReturn(ACTIVE);

        doNothing().doThrow(new ResourceInUseException("exists"))
                .when(kinesis)
                .createStream(eq(STREAM_NAME), anyInt());

        StreamCreateConfiguration create = new StreamCreateConfiguration()
                .retryPeriod(Duration.milliseconds(100))
                .shardCount(10);
        assertThat(callCreateAssertTerminated(create)).isTrue();

        verify(kinesis, times(5)).describeStream(STREAM_NAME);
        verify(kinesis, times(1)).createStream(STREAM_NAME, create.getShardCount());
    }

    @Test
    public void createdAtInconvenientTimeStillWorks() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenThrow(new ResourceNotFoundException("does not exist yet"))
                .thenReturn(NOT_ACTIVE)
                .thenReturn(NOT_ACTIVE)
                .thenReturn(NOT_ACTIVE)
                .thenReturn(ACTIVE);

        doThrow(new ResourceInUseException("exists"))
                .when(kinesis)
                .createStream(eq(STREAM_NAME), anyInt());

        StreamCreateConfiguration create = new StreamCreateConfiguration()
                .retryPeriod(Duration.milliseconds(100));
        assertThat(callCreateAssertTerminated(create)).isTrue();

        verify(kinesis, times(5)).describeStream(STREAM_NAME);
        verify(kinesis, times(1)).createStream(anyString(), anyInt());
    }

    @Test
    public void unexpectedErrorForCreate() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenThrow(new ResourceNotFoundException("does not exist yet"))
                .thenReturn(NOT_ACTIVE)
                .thenReturn(NOT_ACTIVE)
                .thenReturn(NOT_ACTIVE)
                .thenReturn(ACTIVE);

        doThrow(new RuntimeException("exists"))
                .when(kinesis)
                .createStream(eq(STREAM_NAME), anyInt());

        StreamCreateConfiguration create = new StreamCreateConfiguration()
                .retryPeriod(Duration.milliseconds(100));
        assertThat(callCreateAssertTerminated(create)).isTrue();

        verify(kinesis, times(5)).describeStream(STREAM_NAME);
        verify(kinesis, times(1)).createStream(anyString(), anyInt());
    }

    @Test
    public void threadInterruptedExitsLook() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenReturn(NOT_ACTIVE);

        StreamCreateConfiguration create = new StreamCreateConfiguration()
                .retryPeriod(Duration.milliseconds(100));

        AtomicBoolean setup = new AtomicBoolean(false);
        Thread thread = new Thread(() -> setup.set(create.setupStream(kinesis, STREAM_NAME))){
            {
                setDaemon(true);
            }
        };
        thread.start();
        thread.join(200);
        thread.interrupt();

        assertThat(setup.get()).isFalse();

        verify(kinesis, never()).createStream(anyString(), anyInt());
    }

    @Test
    public void maxRetries() throws Exception {
        when(kinesis.describeStream(STREAM_NAME))
                .thenThrow(new ResourceNotFoundException("does not exist yet"));

        doNothing().when(kinesis)
                .createStream(eq(STREAM_NAME), anyInt());

        StreamCreateConfiguration create = new StreamCreateConfiguration()
                .retryPeriod(Duration.milliseconds(10))
                .maxAttempts(10);
        assertThat(callCreateAssertTerminated(create)).isFalse();

        verify(kinesis, times(10)).describeStream(STREAM_NAME);
        verify(kinesis, times(10)).createStream(anyString(), anyInt());
    }

    private boolean callCreateAssertTerminated(final StreamCreateConfiguration create) throws InterruptedException {
        return callCreateAssertTerminated(create, Duration.seconds(2));
    }

    private boolean callCreateAssertTerminated(final StreamCreateConfiguration create, Duration wait) throws InterruptedException {
        final AtomicBoolean result = new AtomicBoolean(false);
        Thread thread = new Thread(() -> result.set(create.setupStream(kinesis, STREAM_NAME))) {
            {
                setDaemon(true);
            }
        };
        thread.start();
        thread.join(wait.toMilliseconds());

        assertThat(thread.getState()).isEqualTo(Thread.State.TERMINATED);
        return result.get();
    }

    public static final class FakeConfiguration extends Configuration {

        @JsonProperty
        @Valid
        public StreamCreateConfiguration create;

    }
}
