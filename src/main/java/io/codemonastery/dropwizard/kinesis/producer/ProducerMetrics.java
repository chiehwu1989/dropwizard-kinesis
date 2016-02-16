package io.codemonastery.dropwizard.kinesis.producer;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class ProducerMetrics {

    private Meter partitionKeySuccessMeter;
    private Meter partitionKeyFailureMeter;
    private Meter encodeFailureMeter;
    private Meter encodeSuccessMeter;

    private Meter sentMeter;
    private Meter failedMeter;
    private Timer putRecordsTimer;

    public ProducerMetrics(MetricRegistry metrics, String name) {
        Preconditions.checkNotNull(name, "name cannot be null");

        if(metrics != null){
            partitionKeySuccessMeter = metrics.meter(name + "-partition-key-success");
            partitionKeyFailureMeter = metrics.meter(name + "-partition-key-failure");
            encodeFailureMeter = metrics.meter(name + "-encode-failure");
            encodeSuccessMeter = metrics.meter(name + "-encode-success");
            sentMeter = metrics.meter(name + "-sent");
            failedMeter = metrics.meter(name + "-failed");
            putRecordsTimer = metrics.timer(name + "-put-records");
        }
    }

    public final void partitionKeyed(){
        if(partitionKeySuccessMeter != null){
            partitionKeySuccessMeter.mark();
        }
    }

    public final void partitionKeyFailed(){
        if(partitionKeyFailureMeter != null){
            partitionKeyFailureMeter.mark();
        }
    }

    public final void encodeFailed(){
        if(encodeFailureMeter != null){
            encodeFailureMeter.mark();
        }
    }

    public void encoded() {
        if(encodeSuccessMeter != null){
            encodeSuccessMeter.mark();
        }
    }

    public final void sent(long numRecords, long numFailedRecords) {
        if(sentMeter != null){
            sentMeter.mark(numRecords);
            failedMeter.mark(numFailedRecords);
        }
    }

    public final Closeable time() {
        return putRecordsTimer == null ? NoOpClose.INSTANCE : putRecordsTimer.time();
    }

    private static final double failureFrequencyThreshold = 0.1;
    public List<String> highFailureMetrics(){
        List<String> failed = new ArrayList<>();

        {
            double partitionKeyFailureFrequency = frequency(partitionKeySuccessMeter, partitionKeyFailureMeter, Meter::getOneMinuteRate);
            if(failureFrequencyThreshold <= partitionKeyFailureFrequency){
                failed.add(String.format("%.2f%% partition key failure", partitionKeyFailureFrequency*100));
            }
        }

        {
            double encodeFailureFrequency = frequency(encodeSuccessMeter, encodeFailureMeter, Meter::getOneMinuteRate);
            if(failureFrequencyThreshold <= encodeFailureFrequency){
                failed.add(String.format("%.2f%% encode failure", encodeFailureFrequency*100));
            }
        }

        {
            double sendFailureFrequency = frequency(sentMeter, failedMeter, Meter::getOneMinuteRate);
            if(failureFrequencyThreshold <= sendFailureFrequency){
                failed.add(String.format("%.2f%% send failure", sendFailureFrequency*100));
            }
        }

        return failed;
    }

    private double frequency(Meter successMeter, Meter failureMeter, Function<Meter, Double> rate) {
        Double successRate = rate.apply(successMeter);
        Double failureRate = rate.apply(failureMeter);
        double totalRate = successRate + failureRate;
        return failureRate / totalRate;
    }
}
