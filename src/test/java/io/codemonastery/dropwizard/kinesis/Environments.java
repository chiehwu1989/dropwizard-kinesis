package io.codemonastery.dropwizard.kinesis;

import com.codahale.metrics.MetricRegistry;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.validation.Validators;
import org.eclipse.jetty.util.component.LifeCycle;
import io.dropwizard.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

public class Environments {

    private static final Logger LOG = LoggerFactory.getLogger(Environments.class);

    public static void run(String applicationName, EnvironmentRunnable runnable) throws Exception {
        Environment env = null;
        try {
            env = new Environment(applicationName, Jackson.newObjectMapper(), Validators.newValidator(), new MetricRegistry(), Environments.class.getClassLoader());
            runnable.run(env);
        } finally {
            if (env != null) {
                Stack<LifeCycle> objects = new Stack<>();
                objects.addAll(env.lifecycle().getManagedObjects());
                while (!objects.empty()) {
                    try {
                        objects.pop().stop();
                    } catch (Exception e) {
                        LOG.error("Unexpected error cleaning up lifecycle", e);
                    }
                }
            }
        }
    }

    private Environments() {
    }
}
