package io.codemonastery.dropwizard.kinesis.metric;


import com.codahale.metrics.Gauge;

import java.util.HashMap;
import java.util.Map;

public class ShardMillisBehindLatest implements Gauge<Map<String, Long>> {

    private final Map<String, Long> millisBehindLatest = new HashMap<>();

    public synchronized void update(String shardId, long millisBehindLatest){
        this.millisBehindLatest.put(shardId, millisBehindLatest);
    }

    public synchronized void remove(String shardId){
        this.millisBehindLatest.remove(shardId);
    }

    @Override
    public Map<String, Long> getValue() {
        return millisBehindLatest;
    }
}
