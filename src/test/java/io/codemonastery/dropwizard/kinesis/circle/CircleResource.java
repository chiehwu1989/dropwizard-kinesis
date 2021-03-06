package io.codemonastery.dropwizard.kinesis.circle;

import io.codemonastery.dropwizard.kinesis.producer.Producer;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CircleResource {

    private final List<String> seen = new ArrayList<>();
    public final Producer<String> producer;

    public CircleResource(Producer<String> producer) {
        this.producer = producer;
    }

    @GET
    public synchronized String[] get(){
        return seen.toArray(new String[seen.size()]);
    }

    @POST
    public void send(String[] sendMe) throws Exception {
        producer.sendAll(Arrays.asList(sendMe));
    }

    @DELETE
    public synchronized void delete(){
        seen.clear();
    }

    public synchronized void seen(String event) {
            seen.add(event);
    }
}
