package io.codemonastery.dropwizard.kinesis;

import io.dropwizard.util.Duration;

public class Assertions {

    public static void retry(int retries, Duration period, AutoCloseable runnable) throws Exception {
        for(int i = 5; i > 0; i--){
            try{
                runnable.close();
                break;
            }catch (AssertionError e){
                if(i == 1){
                    throw e;
                }
            }
            Thread.sleep(100);
        }
    }

    private Assertions() {
    }
}
