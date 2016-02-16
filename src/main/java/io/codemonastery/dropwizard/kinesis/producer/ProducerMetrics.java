package io.codemonastery.dropwizard.kinesis.producer;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;

import java.io.Closeable;

public class ProducerMetrics {

    private Meter partitionKeyFailedMeter;
    private Meter encodeFailedMeter;

    private Meter sentMeter;
    private Meter failedMeter;
    private Timer putRecordsTimer;

    public ProducerMetrics(MetricRegistry metrics, String name) {
        Preconditions.checkNotNull(name, "name cannot be null");

        if(metrics != null){
            partitionKeyFailedMeter = metrics.meter(name + "-partition-key-failed");
            encodeFailedMeter = metrics.meter(name + "-encode-failed");
            sentMeter = metrics.meter(name + "-sent");
            failedMeter = metrics.meter(name + "-failed");
            putRecordsTimer = metrics.timer(name + "-put-records");
        }
    }

    public void partitionkeyFailed(){
        if(partitionKeyFailedMeter != null){
            partitionKeyFailedMeter.mark();
        }
    }

    public void encodeFailed(){
        if(encodeFailedMeter != null){
            encodeFailedMeter.mark();
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
