package io.codemonastery.dropwizard.kinesis;

import io.dropwizard.setup.Environment;

public interface EnvironmentRunnable {

    void run(Environment environment) throws Exception;

}
