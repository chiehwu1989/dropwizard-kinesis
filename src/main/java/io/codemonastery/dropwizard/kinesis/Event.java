package io.codemonastery.dropwizard.kinesis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

public class Event implements Serializable {

    private static final long serialVersionUID = 3094679943267135032L;

    public static void addMetaEventData(Event event) {
    }

    @Override
    public String toString() {
        try {
            return JSON.writeValueAsString(this) + "\n";
        } catch (JsonProcessingException e) {
            return super.toString() + "\n";
        }
    }

    public final static ObjectMapper JSON = new ObjectMapper();

    static {
        JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
}
