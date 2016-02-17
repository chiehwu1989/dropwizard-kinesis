package io.codemonastery.dropwizard.kinesis.metric;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.MockitoAnnotations.initMocks;

public class DynamoDbMetricsProxyTest {

    @Mock
    private AmazonDynamoDB delegate;

    private MetricRegistry metricRegistry;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        this.metricRegistry = new MetricRegistry();
    }

    @Test
    public void batchGetItem() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-get-item-batch").getCount()).isEqualTo(0);
        dynamoDB.batchGetItem((BatchGetItemRequest) null);
        dynamoDB.batchGetItem((Map<String, KeysAndAttributes>) null);
        dynamoDB.batchGetItem(null, null);
        assertThat(metricRegistry.timer("foo-get-item-batch").getCount()).isEqualTo(3);
    }

    @Test
    public void batchWriteItem() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-write-item-batch").getCount()).isEqualTo(0);
        dynamoDB.batchWriteItem((BatchWriteItemRequest) null);
        dynamoDB.batchWriteItem((Map<String, List<WriteRequest>>) null);
        assertThat(metricRegistry.timer("foo-write-item-batch").getCount()).isEqualTo(2);
    }

    @Test
    public void createTable() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-create-table").getCount()).isEqualTo(0);
        dynamoDB.createTable(null);
        dynamoDB.createTable(null, null, null, null);
        assertThat(metricRegistry.timer("foo-create-table").getCount()).isEqualTo(2);
    }

    @Test
    public void deleteItem() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-delete-item").getCount()).isEqualTo(0);
        dynamoDB.deleteItem(null);
        dynamoDB.deleteItem(null, null);
        dynamoDB.deleteItem(null, null, null);
        assertThat(metricRegistry.timer("foo-delete-item").getCount()).isEqualTo(3);
    }

    @Test
    public void deleteTable() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-delete-table").getCount()).isEqualTo(0);
        dynamoDB.deleteTable((DeleteTableRequest) null);
        dynamoDB.deleteTable((String) null);
        assertThat(metricRegistry.timer("foo-delete-table").getCount()).isEqualTo(2);
    }

    @Test
    public void describeTable() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-describe-table").getCount()).isEqualTo(0);
        dynamoDB.describeTable((DescribeTableRequest) null);
        dynamoDB.describeTable((String) null);
        assertThat(metricRegistry.timer("foo-describe-table").getCount()).isEqualTo(2);
    }

    @Test
    public void getItem() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-get-item").getCount()).isEqualTo(0);
        dynamoDB.getItem(null);
        dynamoDB.getItem(null, null);
        dynamoDB.getItem(null, null, null);
        assertThat(metricRegistry.timer("foo-get-item").getCount()).isEqualTo(3);
    }

    @Test
    public void listTables() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-list-tables").getCount()).isEqualTo(0);
        dynamoDB.listTables();
        dynamoDB.listTables((ListTablesRequest) null);
        dynamoDB.listTables((String) null);
        dynamoDB.listTables(null, null);
        dynamoDB.listTables((Integer) null);
        assertThat(metricRegistry.timer("foo-list-tables").getCount()).isEqualTo(5);
    }

    @Test
    public void putItem() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-put-item").getCount()).isEqualTo(0);
        dynamoDB.putItem(null);
        dynamoDB.putItem(null, null);
        dynamoDB.putItem(null, null, null);
        assertThat(metricRegistry.timer("foo-put-item").getCount()).isEqualTo(3);
    }

    @Test
    public void query() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-query").getCount()).isEqualTo(0);
        dynamoDB.query(null);
        assertThat(metricRegistry.timer("foo-query").getCount()).isEqualTo(1);
    }

    @Test
    public void scan() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-scan").getCount()).isEqualTo(0);
        dynamoDB.scan(null);
        dynamoDB.scan(null, (List<String>) null);
        dynamoDB.scan(null, (Map<String, Condition>) null);
        dynamoDB.scan(null, null, null);
        assertThat(metricRegistry.timer("foo-scan").getCount()).isEqualTo(4);
    }

    @Test
    public void updateItem() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-update-item").getCount()).isEqualTo(0);
        dynamoDB.updateItem(null);
        dynamoDB.updateItem(null, null, null);
        dynamoDB.updateItem(null, null, null, null);
        assertThat(metricRegistry.timer("foo-update-item").getCount()).isEqualTo(3);
    }

    @Test
    public void updateTable() throws Exception {
        DynamoDbMetricsProxy dynamoDB = new DynamoDbMetricsProxy(delegate, metricRegistry, "foo");
        assertThat(metricRegistry.timer("foo-update-table").getCount()).isEqualTo(0);
        dynamoDB.updateTable(null);
        dynamoDB.updateTable(null, null);
        assertThat(metricRegistry.timer("foo-update-table").getCount()).isEqualTo(2);
    }
}
