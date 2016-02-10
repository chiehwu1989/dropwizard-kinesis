package io.codemonastery.dropwizard.kinesis.circle;


import io.codemonastery.dropwizard.kinesis.SendService;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;

@Path("/circle")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CircleResource {

    private final List<String> seenMessages = new ArrayList<>();
    private final SendService kinesisSendService;

    public CircleResource(SendService kinesisSendService) {
        this.kinesisSendService = kinesisSendService;
    }

    public void messageSeen(String message){
        synchronized (seenMessages){
            seenMessages.add(message);
        }
    }

    @POST
    public synchronized void sendMessages(String[] messages){
        for (String message : messages) {
            kinesisSendService.sendRecordAsString(message);
        }
    }

    @GET
    public String[] getAllSeenMessages(){
        final String[] response;
        synchronized (seenMessages){
            response = seenMessages.toArray(new String[seenMessages.size()]);
            seenMessages.clear();
        }
        return response;
    }


}
