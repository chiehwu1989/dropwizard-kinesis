package io.codemonastery.dropwizard.kinesis.consumer;

import com.codahale.metrics.Gauge;

class LongGauge implements Gauge<Long> {

    private long value;

    @Override
    public Long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
