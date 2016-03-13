package io.codemonastery.dropwizard.kinesis;

import io.dropwizard.util.Duration;

public class Assertions {

    public static void retry(int retries, Duration period, Runnable runnable) throws Throwable {
        for(int i = retries; i > 0; i--){
            try{
                runnable.run();
                break;
            }catch (AssertionError e){
                if(i == 1){
                    throw e;
                }
            }
            Thread.sleep(period.toMilliseconds());
        }
    }

    public interface Runnable {
        void run() throws Throwable;
    }

    private Assertions() {
    }
}
