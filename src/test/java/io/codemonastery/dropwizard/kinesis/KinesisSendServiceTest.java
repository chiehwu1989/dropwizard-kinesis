package io.codemonastery.dropwizard.kinesis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KinesisSendServiceTest {

    private static final TestEvent TEST_EVENT;

    static {
        try {
            TEST_EVENT = new ObjectMapper().readValue(
                    fixture("fixtures/test.json"),
                    TestEvent.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ScheduledExecutorService flushExecutor;

    @Before
    public void setUp() throws Exception {
        flushExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void tearDown() throws Exception {
        flushExecutor.shutdownNow();
    }

    @Test
    public void bufferEmpty() throws Exception {
        final KinesisSendService testSendService = new TestSendService(1024, 1);
        assertTrue(testSendService.isEmpty());
    }

    @Test
    public void bufferNotFullWaitForFlush() throws Exception {
        final TestSendService testSendService = new TestSendService(1024 * 1024, 1);
        for (int i = 0; i < 4; i++) {
            testSendService.send(TEST_EVENT);
        }
        assertEquals(0, testSendService.flushcalled);

        Thread.sleep(1500); //sleep for longer than a second
        assertEquals(1, testSendService.flushcalled);
    }

    @Test
    public void bufferFilledDefinitelyFlushed() throws Exception {
        final TestSendService testSendService = new TestSendService(256, 1);
        for (int i = 0; i < 2; i++) {
            testSendService.send(TEST_EVENT);
        }
        assertEquals(1, testSendService.flushcalled);

        Thread.sleep(1500); //sleep for longer than a second
        assertEquals(2, testSendService.flushcalled);
    }


    public class TestSendService extends KinesisSendService {

        private int flushcalled = 0;

        public TestSendService(int bufferSize, int flushPeriodSeconds) {
            super(null, "kinesis-send-service-test", bufferSize, flushPeriodSeconds, null, flushExecutor);
        }

        synchronized final void flush() {
            flushcalled++;
            newBuffer();
        }
    }


    private static class TestEvent extends Event {

        private static final long serialVersionUID = 2839163807640058392L;
        private String download_time;
        private Boolean is_retargeting;
        private String device_name;
        private String device_type;

        public TestEvent() {
        }

        @JsonProperty
        public static long getSerialVersionUID() {
            return serialVersionUID;
        }

        @JsonProperty
        public String getDownload_time() {
            return download_time;
        }

        public void setDownload_time(String download_time) {
            this.download_time = download_time;
        }

        @JsonProperty
        public Boolean getIs_retargeting() {
            return is_retargeting;
        }

        public void setIs_retargeting(Boolean is_retargeting) {
            this.is_retargeting = is_retargeting;
        }

        @JsonProperty
        public String getDevice_name() {
            return device_name;
        }

        public void setDevice_name(String device_name) {
            this.device_name = device_name;
        }

        @JsonProperty
        public String getDevice_type() {
            return device_type;
        }

        public void setDevice_type(String device_type) {
            this.device_type = device_type;
        }
    }

}