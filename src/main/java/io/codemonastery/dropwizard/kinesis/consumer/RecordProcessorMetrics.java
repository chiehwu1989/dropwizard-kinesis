package io.codemonastery.dropwizard.kinesis.consumer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.codemonastery.dropwizard.kinesis.producer.NoOpClose;

public class RecordProcessorMetrics {

    public static RecordProcessorMetrics noOp() {
        return new RecordProcessorMetrics(null, "");
    }

    private Counter processorCounter;
    private Meter decodeFailureMeter;
    private Meter successMeter;
    private Meter failureMeter;
    private Timer processTimer;
    private Meter unhandledExceptionMeter;

    public RecordProcessorMetrics(MetricRegistry metrics, String name) {
        if(metrics != null){
            processorCounter = metrics.counter(name + "-processors");
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
}
