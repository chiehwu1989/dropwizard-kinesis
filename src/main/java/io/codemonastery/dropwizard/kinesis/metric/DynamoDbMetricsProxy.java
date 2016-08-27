package io.codemonastery.dropwizard.kinesis.metric;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.util.List;
import java.util.Map;

public class DynamoDbMetricsProxy implements AmazonDynamoDB {

    private final AmazonDynamoDB delegate;

    private final Timer batchGetItemTimer;
    private final Timer batchWriteItemTimer;
    private final Timer createTableTimer;
    private final Timer deleteItemTimer;
    private final Timer deleteTableTimer;
    private final Timer describeTableTimer;
    private final Timer getItemTimer;
    private final Timer listTablesTimer;
    private final Timer putItemTimer;
    private final Timer queryTimer;
    private final Timer scanTimer;
    private final Timer updateItemTimer;
    private final Timer updateTableTimer;
    private final Timer describeLimitsTimer;

    public DynamoDbMetricsProxy(AmazonDynamoDB delegate, MetricRegistry metrics, String name) {
        this.delegate = delegate;
        batchGetItemTimer = metrics.timer(name + "-get-item-batch");
        batchWriteItemTimer = metrics.timer(name + "-write-item-batch");
        createTableTimer = metrics.timer(name + "-create-table");
        deleteItemTimer = metrics.timer(name + "-delete-item");
        deleteTableTimer = metrics.timer(name + "-delete-table");
        describeTableTimer = metrics.timer(name + "-describe-table");
        getItemTimer = metrics.timer(name + "-get-item");
        listTablesTimer = metrics.timer(name + "-list-tables");
        putItemTimer = metrics.timer(name + "-put-item");
        queryTimer = metrics.timer(name + "-query");
        scanTimer = metrics.timer(name + "-scan");
        updateItemTimer = metrics.timer(name + "-update-item");
        updateTableTimer = metrics.timer(name + "-update-table");
        describeLimitsTimer = metrics.timer(name + "-describe-limits");
    }

    @Override
    public void setEndpoint(String s) {
        delegate.setEndpoint(s);
    }

    @Override
    public void setRegion(Region region) {
        delegate.setRegion(region);
    }

    @Override
    public BatchGetItemResult batchGetItem(BatchGetItemRequest batchGetItemRequest) {
        try(Timer.Context ignored = batchGetItemTimer.time()){
            return delegate.batchGetItem(batchGetItemRequest);
        }
    }

    @Override
    public BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> map, String s) {
        try(Timer.Context ignored = batchGetItemTimer.time()) {
            return delegate.batchGetItem(map, s);
        }
    }

    @Override
    public BatchGetItemResult batchGetItem(Map<String, KeysAndAttributes> map) {
        try(Timer.Context ignored = batchGetItemTimer.time()) {
            return delegate.batchGetItem(map);
        }
    }

    @Override
    public BatchWriteItemResult batchWriteItem(BatchWriteItemRequest batchWriteItemRequest) {
        try(Timer.Context ignored = batchWriteItemTimer.time()) {
            return delegate.batchWriteItem(batchWriteItemRequest);
        }
    }

    @Override
    public BatchWriteItemResult batchWriteItem(Map<String, List<WriteRequest>> map) {
        try(Timer.Context ignored = batchWriteItemTimer.time()) {
            return delegate.batchWriteItem(map);
        }
    }

    @Override
    public CreateTableResult createTable(CreateTableRequest createTableRequest) {
        try(Timer.Context ignored = createTableTimer.time()) {
            return delegate.createTable(createTableRequest);
        }
    }

    @Override
    public CreateTableResult createTable(List<AttributeDefinition> list, String s, List<KeySchemaElement> list1, ProvisionedThroughput provisionedThroughput) {
        try(Timer.Context ignored = createTableTimer.time()) {
            return delegate.createTable(list, s, list1, provisionedThroughput);
        }
    }

    @Override
    public DeleteItemResult deleteItem(DeleteItemRequest deleteItemRequest) {
        try(Timer.Context ignored = deleteItemTimer.time()) {
            return delegate.deleteItem(deleteItemRequest);
        }
    }

    @Override
    public DeleteItemResult deleteItem(String s, Map<String, AttributeValue> map) {
        try(Timer.Context ignored = deleteItemTimer.time()) {
            return delegate.deleteItem(s, map);
        }
    }

    @Override
    public DeleteItemResult deleteItem(String s, Map<String, AttributeValue> map, String s1) {
        try(Timer.Context ignored = deleteItemTimer.time()) {
            return delegate.deleteItem(s, map, s1);
        }
    }

    @Override
    public DeleteTableResult deleteTable(DeleteTableRequest deleteTableRequest) {
        try(Timer.Context ignored = deleteTableTimer.time()) {
            return delegate.deleteTable(deleteTableRequest);
        }
    }

    @Override
    public DeleteTableResult deleteTable(String s) {
        try(Timer.Context ignored = deleteTableTimer.time()) {
            return delegate.deleteTable(s);
        }
    }

    @Override
    public DescribeLimitsResult describeLimits(DescribeLimitsRequest describeLimitsRequest) {
        try(Timer.Context ignored = describeLimitsTimer.time()){
            return delegate.describeLimits(describeLimitsRequest);
        }

    }

    @Override
    public DescribeTableResult describeTable(DescribeTableRequest describeTableRequest) {
        try(Timer.Context ignored = describeTableTimer.time()) {
            return delegate.describeTable(describeTableRequest);
        }
    }

    @Override
    public DescribeTableResult describeTable(String s) {
        try(Timer.Context ignored = describeTableTimer.time()) {
            return delegate.describeTable(s);
        }
    }

    @Override
    public GetItemResult getItem(GetItemRequest getItemRequest) {
        try(Timer.Context ignored = getItemTimer.time()) {
            return delegate.getItem(getItemRequest);
        }
    }

    @Override
    public GetItemResult getItem(String s, Map<String, AttributeValue> map) {
        try(Timer.Context ignored = getItemTimer.time()) {
            return delegate.getItem(s, map);
        }
    }

    @Override
    public GetItemResult getItem(String s, Map<String, AttributeValue> map, Boolean aBoolean) {
        try(Timer.Context ignored = getItemTimer.time()) {
            return delegate.getItem(s, map, aBoolean);
        }
    }

    @Override
    public ListTablesResult listTables(ListTablesRequest listTablesRequest) {
        try(Timer.Context ignored = listTablesTimer.time()) {
            return delegate.listTables(listTablesRequest);
        }
    }

    @Override
    public ListTablesResult listTables() {
        try(Timer.Context ignored = listTablesTimer.time()) {
            return delegate.listTables();
        }
    }

    @Override
    public ListTablesResult listTables(String s) {
        try(Timer.Context ignored = listTablesTimer.time()) {
            return delegate.listTables(s);
        }
    }

    @Override
    public ListTablesResult listTables(String s, Integer integer) {
        try(Timer.Context ignored = listTablesTimer.time()) {
            return delegate.listTables(s, integer);
        }
    }

    @Override
    public ListTablesResult listTables(Integer integer) {
        try(Timer.Context ignored = listTablesTimer.time()) {
            return delegate.listTables(integer);
        }
    }

    @Override
    public PutItemResult putItem(PutItemRequest putItemRequest) {
        try(Timer.Context ignored = putItemTimer.time()) {
            return delegate.putItem(putItemRequest);
        }
    }

    @Override
    public PutItemResult putItem(String s, Map<String, AttributeValue> map) {
        try(Timer.Context ignored = putItemTimer.time()) {
            return delegate.putItem(s, map);
        }
    }

    @Override
    public PutItemResult putItem(String s, Map<String, AttributeValue> map, String s1) {
        try(Timer.Context ignored = putItemTimer.time()) {
            return delegate.putItem(s, map, s1);
        }
    }

    @Override
    public QueryResult query(QueryRequest queryRequest) {
        try(Timer.Context ignored = queryTimer.time()) {
            return delegate.query(queryRequest);
        }
    }

    @Override
    public ScanResult scan(ScanRequest scanRequest) {
        try(Timer.Context ignored = scanTimer.time()) {
            return delegate.scan(scanRequest);
        }
    }

    @Override
    public ScanResult scan(String s, List<String> list) {
        try(Timer.Context ignored = scanTimer.time()) {
            return delegate.scan(s, list);
        }
    }

    @Override
    public ScanResult scan(String s, Map<String, Condition> map) {
        try(Timer.Context ignored = scanTimer.time()) {
            return delegate.scan(s, map);
        }
    }

    @Override
    public ScanResult scan(String s, List<String> list, Map<String, Condition> map) {
        try(Timer.Context ignored = scanTimer.time()) {
            return delegate.scan(s, list, map);
        }
    }

    @Override
    public UpdateItemResult updateItem(UpdateItemRequest updateItemRequest) {
        try(Timer.Context ignored = updateItemTimer.time()) {
            return delegate.updateItem(updateItemRequest);
        }
    }

    @Override
    public UpdateItemResult updateItem(String s, Map<String, AttributeValue> map, Map<String, AttributeValueUpdate> map1) {
        try(Timer.Context ignored = updateItemTimer.time()) {
            return delegate.updateItem(s, map, map1);
        }
    }

    @Override
    public UpdateItemResult updateItem(String s, Map<String, AttributeValue> map, Map<String, AttributeValueUpdate> map1, String s1) {
        try(Timer.Context ignored = updateItemTimer.time()) {
            return delegate.updateItem(s, map, map1, s1);
        }
    }

    @Override
    public UpdateTableResult updateTable(UpdateTableRequest updateTableRequest) {
        try(Timer.Context ignored = updateTableTimer.time()) {
            return delegate.updateTable(updateTableRequest);
        }
    }

    @Override
    public UpdateTableResult updateTable(String s, ProvisionedThroughput provisionedThroughput) {
        try(Timer.Context ignored = updateTableTimer.time()) {
            return delegate.updateTable(s, provisionedThroughput);
        }
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
    }

    @Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest amazonWebServiceRequest) {
        return delegate.getCachedResponseMetadata(amazonWebServiceRequest);
    }
}
