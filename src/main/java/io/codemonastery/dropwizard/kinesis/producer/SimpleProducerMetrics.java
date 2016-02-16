package io.codemonastery.dropwizard.kinesis.producer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;

import java.io.Closeable;

public class SimpleProducerMetrics {

    private Meter sentMeter;
    private Meter failedMeter;
    private Timer putRecordsTimer;

    public SimpleProducerMetrics(MetricRegistry metrics, String name) {
        Preconditions.checkNotNull(name, "name cannot be null");

        if(metrics != null){
            sentMeter = metrics.meter(name + "-sent");
            failedMeter = metrics.meter(name + "-failed");
            putRecordsTimer = metrics.timer(name + "-put-records");
        }
    }

    public void sent(long numRecords, long numFailedRecords) {
        if(sentMeter != null){
            sentMeter.mark(numRecords);
            failedMeter.mark(numFailedRecords);
        }
    }

    public Closeable time() {
        return putRecordsTimer == null ? NoOpClose.INSTANCE : putRecordsTimer.time();
    }
}
