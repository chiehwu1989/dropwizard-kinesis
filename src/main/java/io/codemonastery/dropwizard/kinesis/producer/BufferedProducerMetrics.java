package io.codemonastery.dropwizard.kinesis.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

public class BufferedProducerMetrics extends SimpleProducerMetrics {


    private Meter bufferPutMeter;
    private Counter bufferSizeCounter;

    public BufferedProducerMetrics(MetricRegistry metrics, String name) {
        super(metrics, name);
        Preconditions.checkNotNull(name, "name cannot be null");

       if(metrics != null){
           bufferPutMeter = metrics.meter("buffer-put");
           bufferSizeCounter = metrics.counter("buffer-size");
       }
    }

    public void bufferPut(int n){
        if(bufferPutMeter != null){
            bufferPutMeter.mark(n);
            bufferSizeCounter.inc(n);
        }
    }

    public void bufferRemove(int n){
        if(bufferSizeCounter != null){
            bufferSizeCounter.dec(n);
        }
    }
}
