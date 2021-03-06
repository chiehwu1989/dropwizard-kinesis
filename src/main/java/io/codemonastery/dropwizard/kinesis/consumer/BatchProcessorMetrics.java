package io.codemonastery.dropwizard.kinesis.consumer;

import com.codahale.metrics.*;
import io.codemonastery.dropwizard.kinesis.metric.HasFailureThresholds;
import io.codemonastery.dropwizard.kinesis.producer.NoOpClose;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchProcessorMetrics implements HasFailureThresholds {

    public static BatchProcessorMetrics noOp() {
        return new BatchProcessorMetrics(null, "");
    }

    private final MetricRegistry metrics;
    private final String name;
    private Counter processorCounter;
    private Meter decodeSuccessMeter;
    private Meter decodeFailureMeter;
    private Meter successMeter;
    private Meter batchSuccessMeter;
    private Meter failureMeter;
    private Meter batchFailureMeter;
    private Timer processTimer;
    private Timer checkpointTimer;
    private Meter checkpointFailure;
    private Meter unhandledExceptionMeter;
    private final Map<String, LongGauge> millisBehindLatest = new HashMap<>();

    public BatchProcessorMetrics(MetricRegistry metrics, String name) {
        this.metrics = metrics;
        this.name = name;
        if(metrics != null){
            processorCounter = metrics.counter(name + "-processors");
            decodeSuccessMeter = metrics.meter(name + "-decode-success");
            decodeFailureMeter = metrics.meter(name + "-decode-failure");
            successMeter = metrics.meter(name + "-success");
            batchSuccessMeter = metrics.meter(name + "-batch-success");
            failureMeter = metrics.meter(name + "-failure");
            batchFailureMeter = metrics.meter(name + "-batch-failure");
            processTimer = metrics.timer(name + "-process");
            checkpointTimer = metrics.timer(name + "-checkpoint");
            checkpointFailure = metrics.meter(name + "-checkpoint-failure");
            unhandledExceptionMeter = metrics.meter(name + "-unhandled-exception");
        }
    }

    public synchronized void processorStarted() {
        if(processorCounter != null){
            processorCounter.inc();
        }
    }

    public synchronized void processorShutdown(String shardId) {
        if(processorCounter != null){
            processorCounter.dec();
        }
        if(metrics != null && shardId != null){
            millisBehindLatest.remove(shardId);
            metrics.remove(millisBehindLatestName(shardId));
        }
    }

    public void decoded() {
        if(decodeSuccessMeter != null){
            decodeSuccessMeter.mark();
            unhandledExceptionMeter.mark();
        }
    }

    public void decodeFailure() {
        if(decodeFailureMeter != null){
            decodeFailureMeter.mark();
            unhandledExceptionMeter.mark();
        }
    }

    public void processSuccess(int size) {
        if(successMeter != null){
            successMeter.mark(size);
        }
        if(batchSuccessMeter != null){
            batchSuccessMeter.mark();
        }
    }

    public void processFailure(int size) {
        if(failureMeter != null){
            failureMeter.mark(size);
        }
        if(batchFailureMeter != null){
            batchFailureMeter.mark();
        }
    }

    public void millisBehindLatest(String shardId, long millis) {
        if(metrics != null  && shardId != null){
            millisBehindLatest(shardId).setValue(millis);
        }
    }

    public void unhandledException(){
        if(unhandledExceptionMeter != null){
            unhandledExceptionMeter.mark();
        }
    }

    public AutoCloseable processTime(){
        return processTimer == null ? NoOpClose.INSTANCE : processTimer.time();
    }

    public AutoCloseable checkpointTime(){
        return checkpointTimer == null ? NoOpClose.INSTANCE : checkpointTimer.time();
    }

    public void checkpointFailed(){
        checkpointFailure.mark();
    }

    private synchronized LongGauge millisBehindLatest(String shardId) {
        LongGauge longGauge = millisBehindLatest.get(shardId);
        if(longGauge == null){
            longGauge = new LongGauge();
            metrics.register(millisBehindLatestName(shardId), longGauge);
            millisBehindLatest.put(shardId, longGauge);
        }
        return longGauge;
    }

    private String millisBehindLatestName(String shardId) {
        return name + "-millis-behind-latest-" + shardId;
    }

    private static final double failureFrequencyThreshold = 0.1;

    @Override
    public List<String> highFailureMetrics(){
        List<String> failed = new ArrayList<>();

        {
            double encodeFailureFrequency = frequency(decodeSuccessMeter, decodeFailureMeter);
            if(failureFrequencyThreshold <= encodeFailureFrequency){
                failed.add(String.format("%.2f%% decode failure", encodeFailureFrequency*100));
            }
        }

        {
            double processFailureFrequency = frequency(successMeter, failureMeter);
            if(failureFrequencyThreshold <= processFailureFrequency){
                failed.add(String.format("%.2f%% process failure", processFailureFrequency*100));
            }
        }

        {
            double checkpointFailureFrequency = checkpointFailure.getOneMinuteRate() / checkpointTimer.getOneMinuteRate();
            if(failureFrequencyThreshold <= checkpointFailureFrequency){
                failed.add(String.format("%.2f%% checkpoint failure", checkpointFailureFrequency*100));
            }
        }

        return failed;
    }

    private double frequency(Metered successMeter, Metered failureMeter) {
        Double successRate = successMeter.getOneMinuteRate();
        Double failureRate = failureMeter.getOneMinuteRate();
        double totalRate = successRate + failureRate;
        return failureRate / totalRate;
    }

}

