package io.codemonastery.dropwizard.kinesis;


public interface SendService {

    void send(Event event);

}
