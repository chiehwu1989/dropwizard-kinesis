package io.codemonastery.dropwizard.kinesis;

import io.dropwizard.util.Duration;

public class Assertions {

    public static void retry(int retries, Duration period, AutoCloseable runnable) throws Exception {
        for(int i = retries; i > 0; i--){
            try{
                runnable.close();
                break;
            }catch (AssertionError e){
                if(i == 1){
                    throw e;
                }
            }
            Thread.sleep(period.toMilliseconds());
        }
    }

    private Assertions() {
    }
}
