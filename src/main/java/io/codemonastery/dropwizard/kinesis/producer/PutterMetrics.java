package io.codemonastery.dropwizard.kinesis.producer;

import java.io.Closeable;

public interface PutterMetrics {

    Closeable time();

    void sent(long successCount, long failedCount);
}
