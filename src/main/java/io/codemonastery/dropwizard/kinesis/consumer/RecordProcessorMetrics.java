package io.codemonastery.dropwizard.kinesis.consumer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.codemonastery.dropwizard.kinesis.metric.HasFailureThresholds;
import io.codemonastery.dropwizard.kinesis.producer.NoOpClose;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class RecordProcessorMetrics implements HasFailureThresholds {

    public static RecordProcessorMetrics noOp() {
        return new RecordProcessorMetrics(null, "");
    }

    private Counter processorCounter;
    private Meter decodeSuccessMeter;
    private Meter decodeFailureMeter;
    private Meter successMeter;
    private Meter failureMeter;
    private Timer processTimer;
    private Meter unhandledExceptionMeter;

    public RecordProcessorMetrics(MetricRegistry metrics, String name) {
        if(metrics != null){
            processorCounter = metrics.counter(name + "-processors");
            decodeSuccessMeter = metrics.meter(name + "-decode-success");
            decodeFailureMeter = metrics.meter(name + "-decode-failure");
            successMeter = metrics.meter(name + "-success");
            failureMeter = metrics.meter(name + "-failure");
            processTimer = metrics.timer(name + "-process-time");
            unhandledExceptionMeter = metrics.meter(name + "-unhandled-exception");
        }
    }

    public void processorStarted() {
        if(processorCounter != null){
            processorCounter.inc();
        }
    }

    public void processorShutdown() {
        if(processorCounter != null){
            processorCounter.dec();
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

    public void processSuccess() {
        if(successMeter != null){
            successMeter.mark();
        }
    }

    public void processFailure() {
        if(failureMeter != null){
            failureMeter.mark();
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


    private static final double failureFrequencyThreshold = 0.1;

    @Override
    public List<String> highFailureMetrics(){
        List<String> failed = new ArrayList<>();

        {
            double encodeFailureFrequency = frequency(decodeSuccessMeter, decodeFailureMeter, Meter::getOneMinuteRate);
            if(failureFrequencyThreshold <= encodeFailureFrequency){
                failed.add(String.format("%.2f%% decode failure", encodeFailureFrequency*100));
            }
        }

        {
            double processFailureFrequency = frequency(successMeter, failureMeter, Meter::getOneMinuteRate);
            if(failureFrequencyThreshold <= processFailureFrequency){
                failed.add(String.format("%.2f%% process failure", processFailureFrequency*100));
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
