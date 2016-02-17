package io.codemonastery.dropwizard.kinesis.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

public class BufferedProducerMetrics extends ProducerMetrics {

    private Meter bufferPutMeter;
    private Counter bufferSizeCounter;

    public BufferedProducerMetrics(MetricRegistry metrics, String name) {
        super(metrics, name);
        Preconditions.checkNotNull(name, "name cannot be null");

       if(metrics != null){
           bufferPutMeter = metrics.meter(name + "-buffer-put");
           bufferSizeCounter = metrics.counter(name + "-buffer-size");
       }
    }

    public final void bufferPut(int n){
        if(bufferPutMeter != null){
            bufferPutMeter.mark(n);
            bufferSizeCounter.inc(n);
        }
    }

    public final void bufferRemove(int n){
        if(bufferSizeCounter != null){
            bufferSizeCounter.dec(n);
        }
    }
}
