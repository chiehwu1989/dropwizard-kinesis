package com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(Enclosed.class)
public class SimpleKinesisProxyTest {

    private static final String STREAM_NAME = "test-stream";

    private static class KinesisSetup {
        @Mock
        protected AmazonKinesis kinesis;

        @Before
        public void setUp() throws Exception {
            initMocks(this);
        }
    }

    public static class Get extends KinesisSetup {

        @Test
        public void get() throws Exception {
            String shardIterator = "shard0-0";
            int maxRecords = 10;
            SimpleKinesisProxy proxy = new SimpleKinesisProxy(kinesis, STREAM_NAME);
            proxy.get(shardIterator, maxRecords);
            verify(kinesis, times(1)).getRecords(matches(shardIterator, maxRecords));
        }

        private static GetRecordsRequest matches(final String shardIterator, final int maxRecords) {
            return argThat(new ArgumentMatcher<GetRecordsRequest>() {
                @Override
                public boolean matches(Object o) {
                    boolean matches = false;
                    if (o instanceof GetRecordsRequest) {
                        GetRecordsRequest request = (GetRecordsRequest) o;
                        matches = Objects.equals(shardIterator, request.getShardIterator()) &&
                                Objects.equals(maxRecords, request.getLimit());
                    }
                    return matches;
                }
            });
        }
    }

    public static class GetStreamInto extends KinesisSetup {
        @Test
        public void active() throws Exception {
            DescribeStreamResult expectedResult = new DescribeStreamResult()
                    .withStreamDescription(
                            new StreamDescription()
                                    .withStreamName(STREAM_NAME)
                                    .withStreamStatus(StreamStatus.ACTIVE));
            when(kinesis.describeStream(any(DescribeStreamRequest.class)))
                    .thenReturn(expectedResult);

            String shardId = "shard0";
            SimpleKinesisProxy proxy = new SimpleKinesisProxy(kinesis, STREAM_NAME);
            DescribeStreamResult actualResult = proxy.getStreamInfo(shardId);
            assertThat(actualResult).isSameAs(expectedResult);
            verify(kinesis, times(1)).describeStream(matches(STREAM_NAME, shardId));
        }

        @Test
        public void updating() throws Exception {
            DescribeStreamResult expectedResult = new DescribeStreamResult()
                    .withStreamDescription(
                            new StreamDescription()
                                    .withStreamName(STREAM_NAME)
                                    .withStreamStatus(StreamStatus.UPDATING));
            when(kinesis.describeStream(any(DescribeStreamRequest.class)))
                    .thenReturn(expectedResult);

            String shardId = "shard0";
            SimpleKinesisProxy proxy = new SimpleKinesisProxy(kinesis, STREAM_NAME);
            DescribeStreamResult actualResult = proxy.getStreamInfo(shardId);
            assertThat(actualResult).isSameAs(expectedResult);
            verify(kinesis, times(1)).describeStream(matches(STREAM_NAME, shardId));
        }

        @Test
        public void notActiveOrUpdating() throws Exception {
            DescribeStreamResult result = new DescribeStreamResult()
                    .withStreamDescription(
                            new StreamDescription()
                                    .withStreamName(STREAM_NAME)
                                    .withStreamStatus(StreamStatus.CREATING));
            when(kinesis.describeStream(any(DescribeStreamRequest.class)))
                    .thenReturn(result);

            String shardId = "shard0";
            SimpleKinesisProxy proxy = new SimpleKinesisProxy(kinesis, STREAM_NAME);
            DescribeStreamResult actualResult = proxy.getStreamInfo(shardId);
            assertThat(actualResult).isNull();
            verify(kinesis, times(1)).describeStream(matches(STREAM_NAME, shardId));
        }

        @Test
        public void limitExceededEventualSuccess() throws Exception {
            DescribeStreamResult expectedResult = new DescribeStreamResult()
                    .withStreamDescription(
                            new StreamDescription()
                                    .withStreamName(STREAM_NAME)
                                    .withStreamStatus(StreamStatus.ACTIVE));
            when(kinesis.describeStream(any(DescribeStreamRequest.class)))
                    .thenThrow(new LimitExceededException("first time"))
                    .thenThrow(new LimitExceededException("second time"))
                    .thenThrow(new LimitExceededException("third time"))
                    .thenThrow(new LimitExceededException("fourth time"))
                    .thenReturn(expectedResult);

            String shardId = "shard0";
            SimpleKinesisProxy proxy = new SimpleKinesisProxy(kinesis, STREAM_NAME, 10, 4);
            DescribeStreamResult actualResult = proxy.getStreamInfo(shardId);
            assertThat(actualResult).isSameAs(expectedResult);
            verify(kinesis, times(5)).describeStream(matches(STREAM_NAME, shardId));
        }

        @Test
        public void limitExceededEventualFailure() throws Exception {
            when(kinesis.describeStream(any(DescribeStreamRequest.class)))
                    .thenThrow(new LimitExceededException("always fails"));

            String shardId = "shard0";
            SimpleKinesisProxy proxy = new SimpleKinesisProxy(kinesis, STREAM_NAME, 10, 4);
            DescribeStreamResult actualResult = proxy.getStreamInfo(shardId);
            assertThat(actualResult).isNull();
            verify(kinesis, times(5)).describeStream(matches(STREAM_NAME, shardId));
        }

        private static DescribeStreamRequest matches(final String streamName, final String shardId) {
            return argThat(new ArgumentMatcher<DescribeStreamRequest>() {
                @Override
                public boolean matches(Object o) {
                    boolean matches = false;
                    if (o instanceof DescribeStreamRequest) {
                        DescribeStreamRequest request = (DescribeStreamRequest) o;
                        matches = Objects.equals(streamName, request.getStreamName()) &&
                                Objects.equals(shardId, request.getExclusiveStartShardId());
                    }
                    return matches;
                }
            });
        }
    }

    public static class GetShardList {

        @Mock
        private SimpleKinesisProxy proxy;

        @Before
        public void setUp() throws Exception {
            initMocks(this);
            when(proxy.getShardList()).thenCallRealMethod();
        }

        @Test
        public void firstShardIsNull() throws Exception {
            ImmutableList<Shard> shards = ImmutableList.of(
                    new Shard().withShardId("shard0"),
                    new Shard().withShardId("shard1"),
                    new Shard().withShardId("shard2"));
            when(proxy.getStreamInfo(isNull(String.class)))
                    .thenReturn(result(shards, false));

            List<Shard> shardList = proxy.getShardList();
            assertThat(shardList).isEqualTo(shards);
            verify(proxy, times(1)).getStreamInfo(anyString());
        }

        @Test
        public void ifHasMoreGetMoreShards() throws Exception {
            ImmutableList<Shard> shards = ImmutableList.of(
                    new Shard().withShardId("shard0"),
                    new Shard().withShardId("shard1"),
                    new Shard().withShardId("shard2"));
            when(proxy.getStreamInfo(isNull(String.class)))
                    .thenReturn(result(shards.subList(0, 2), true));
            when(proxy.getStreamInfo(eq(shards.get(1).getShardId())))
                    .thenReturn(result(shards.subList(2, 3), false));

            List<Shard> shardList = proxy.getShardList();
            assertThat(shardList).isEqualTo(shards);
            verify(proxy, times(2)).getStreamInfo(anyString());
        }

        @Test
        public void noStreamInfo() throws Exception {
            when(proxy.getStreamInfo(anyString()))
                    .thenReturn(null);

            List<Shard> shardList = proxy.getShardList();
            assertThat(shardList).isNull();
            verify(proxy, times(1)).getStreamInfo(anyString());
        }

        private DescribeStreamResult result(ImmutableList<Shard> shards, boolean hasMoreShards) {
            return new DescribeStreamResult()
                    .withStreamDescription(
                            new StreamDescription()
                                    .withStreamName(STREAM_NAME)
                                    .withStreamStatus(StreamStatus.ACTIVE)
                                    .withShards(shards)
                                    .withHasMoreShards(hasMoreShards));
        }
    }

    public static class GetShardIds {

        @Mock
        private SimpleKinesisProxy proxy;

        @Before
        public void setUp() throws Exception {
            initMocks(this);
            when(proxy.getAllShardIds()).thenCallRealMethod();
        }

        @Test
        public void returnsShards() throws Exception {
            ImmutableList<Shard> shards = ImmutableList.of(
                    new Shard().withShardId("shard0"),
                    new Shard().withShardId("shard1"),
                    new Shard().withShardId("shard2"));
            when(proxy.getShardList()).thenReturn(shards);

            Set<String> shardIds = proxy.getAllShardIds();
            assertThat(shardIds).isEqualTo(ImmutableSet.of("shard0", "shard1", "shard2"));
        }

        @Test
        public void returnsNull() throws Exception {
            when(proxy.getShardList()).thenReturn(null);

            Set<String> shardIds = proxy.getAllShardIds();
            assertThat(shardIds).isNull();
        }
    }

    public static class GetShard {

        @Mock
        private SimpleKinesisProxy proxy;

        @Before
        public void setUp() throws Exception {
            initMocks(this);
            when(proxy.getShard(anyString())).thenCallRealMethod();
        }

        @Test
        public void firstCallIsPresent() throws Exception {
            ImmutableList<Shard> shards = ImmutableList.of(
                    new Shard().withShardId("shard0"),
                    new Shard().withShardId("shard1"),
                    new Shard().withShardId("shard2"));
            when(proxy.getListOfShardsSinceLastGet())
                    .thenReturn(null);
            when(proxy.getShardList())
                    .thenReturn(shards);

            Shard shard = proxy.getShard("shard1");
            assertThat(shard).isEqualTo(shards.get(1));
            verify(proxy, times(1)).getListOfShardsSinceLastGet();
            verify(proxy, times(1)).getShardList();
        }

        @Test
        public void firstCallNotPresent() throws Exception {
            ImmutableList<Shard> shards = ImmutableList.of(
                    new Shard().withShardId("shard0"),
                    new Shard().withShardId("shard1"),
                    new Shard().withShardId("shard2"));
            when(proxy.getListOfShardsSinceLastGet())
                    .thenReturn(null);
            when(proxy.getShardList())
                    .thenReturn(shards);

            Shard shard = proxy.getShard("shard3");
            assertThat(shard).isNull();
            verify(proxy, times(1)).getListOfShardsSinceLastGet();
            verify(proxy, times(1)).getShardList();
        }

        @Test
        public void notFirstCallIsPresent() throws Exception {
            ImmutableList<Shard> shards = ImmutableList.of(
                    new Shard().withShardId("shard0"),
                    new Shard().withShardId("shard1"),
                    new Shard().withShardId("shard2"));
            when(proxy.getListOfShardsSinceLastGet())
                    .thenReturn(shards);

            Shard shard = proxy.getShard("shard1");
            assertThat(shard).isEqualTo(shards.get(1));
            verify(proxy, times(1)).getListOfShardsSinceLastGet();
            verify(proxy, never()).getShardList();
        }

        @Test
        public void notFirstCallIsNotPresent() throws Exception {
            ImmutableList<Shard> shards = ImmutableList.of(
                    new Shard().withShardId("shard0"),
                    new Shard().withShardId("shard1"),
                    new Shard().withShardId("shard2"));
            when(proxy.getListOfShardsSinceLastGet())
                    .thenReturn(shards);

            Shard shard = proxy.getShard("shard3");
            assertThat(shard).isNull();
            verify(proxy, times(1)).getListOfShardsSinceLastGet();
            verify(proxy, never()).getShardList();
        }
    }

    public static class GetIterator extends KinesisSetup {

        @Test
        public void get() throws Exception {
            GetShardIteratorResult expectedResult = new GetShardIteratorResult()
                    .withShardIterator("iterator0");
            when(kinesis.getShardIterator(any(GetShardIteratorRequest.class)))
                    .thenReturn(expectedResult);

            SimpleKinesisProxy proxy = new SimpleKinesisProxy(kinesis, STREAM_NAME);

            String actualResult = proxy.getIterator("shard0", "AT_SEQUENCE_NUMBER", "seq0");
            assertThat(actualResult).isEqualTo(expectedResult.getShardIterator());
            verify(kinesis, times(1)).getShardIterator(matches(STREAM_NAME, "shard0", "AT_SEQUENCE_NUMBER", "seq0"));
        }

        private static GetShardIteratorRequest matches(final String streamName,
                                                       final String shardId,
                                                       final String iteratorType,
                                                       final String sequenceNumber) {
            return argThat(new ArgumentMatcher<GetShardIteratorRequest>() {
                @Override
                public boolean matches(Object o) {
                    boolean matches = false;
                    if (o instanceof GetShardIteratorRequest) {
                        GetShardIteratorRequest request = (GetShardIteratorRequest) o;
                        matches = Objects.equals(streamName, request.getStreamName()) &&
                                Objects.equals(shardId, request.getShardId()) &&
                                Objects.equals(iteratorType, request.getShardIteratorType()) &&
                                Objects.equals(sequenceNumber, request.getStartingSequenceNumber());
                    }
                    return matches;
                }
            });
        }

    }

    public static class Put extends KinesisSetup {

        @Test
        public void put() throws Exception {
            PutRecordResult expectedResult = new PutRecordResult();
            when(kinesis.putRecord(any(PutRecordRequest.class)))
                    .thenReturn(expectedResult);

            SimpleKinesisProxy proxy = new SimpleKinesisProxy(kinesis, STREAM_NAME);

            ByteBuffer data = ByteBuffer.allocate(1);
            PutRecordResult actualResult = proxy.put("emsq", "ehk", "pk", data);
            assertThat(actualResult).isSameAs(expectedResult);
            verify(kinesis, times(1)).putRecord(matches(STREAM_NAME, "emsq", "ehk", "pk", data));
        }

        private static PutRecordRequest matches(final String streamName,
                                                final String exclusiveMinimumSequenceNumber,
                                                final String explicitHashKey,
                                                final String partitionKey,
                                                final ByteBuffer data) {
            return argThat(new ArgumentMatcher<PutRecordRequest>() {
                @Override
                public boolean matches(Object o) {
                    boolean matches = false;
                    if (o instanceof PutRecordRequest) {
                        PutRecordRequest request = (PutRecordRequest) o;
                        matches = Objects.equals(streamName, request.getStreamName()) &&
                                Objects.equals(exclusiveMinimumSequenceNumber, request.getSequenceNumberForOrdering()) &&
                                Objects.equals(explicitHashKey, request.getExplicitHashKey()) &&
                                Objects.equals(partitionKey, request.getPartitionKey()) &&
                                data == request.getData();
                    }
                    return matches;
                }
            });
        }

    }
}
