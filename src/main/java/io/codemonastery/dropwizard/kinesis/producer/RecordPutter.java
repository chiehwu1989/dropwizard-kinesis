package io.codemonastery.dropwizard.kinesis.producer;

import com.amazonaws.services.kinesis.model.PutRecordsRequest;

public interface RecordPutter {

    int send(PutRecordsRequest request) throws Exception;

}
