package io.codemonastery.dropwizard.kinesis.lifecycle;

import com.amazonaws.services.kinesis.AmazonKinesis;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

public class ManagedKinesisClientTest {

    @Mock
    private AmazonKinesis kinesis;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void testName() throws Exception {
        ManagedKinesisClient managed = new ManagedKinesisClient(kinesis);
        managed.start();
        verify(kinesis, never()).shutdown();
        managed.stop();
        verify(kinesis, times(1)).shutdown();
    }

}
