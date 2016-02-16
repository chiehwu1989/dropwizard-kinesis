package io.codemonastery.dropwizard.kinesis.lifecycle;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;


public class ManagedDynamoDbClientTest {

    @Mock
    private AmazonDynamoDB dynamoDB;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void testName() throws Exception {
        ManagedDynamoDbClient managed = new ManagedDynamoDbClient(dynamoDB);
        managed.start();
        verify(dynamoDB, never()).shutdown();
        managed.stop();
        verify(dynamoDB, times(1)).shutdown();
    }
}
