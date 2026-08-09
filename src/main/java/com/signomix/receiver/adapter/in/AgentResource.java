package com.signomix.receiver.adapter.in;

import com.signomix.receiver.IotDataMessageCodec;
import com.signomix.receiver.ReceiverService;
import io.quarkus.runtime.StartupEvent;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.OPTIONS;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@Path("/api")
@ApplicationScoped
public class AgentResource {

    @Inject
    Logger LOG;

    @Inject
    EventBus bus;

    @Inject
    ReceiverService service;

    @ConfigProperty(name = "device.authorization.required")
    Boolean authorizationRequired;

    // tab sign as separtor
    private static final String SEPARATOR = "\t";

    public void onApplicationStart(@Observes StartupEvent event) {
        try {
            bus.registerCodec(new IotDataMessageCodec());
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }
    }

    @Path("/receiver/agent")
    @OPTIONS
    public String sendOKString() {
        return "OK";
    }

    @Path("/receiver/agent")
    @POST
    @Produces(MediaType.TEXT_PLAIN)
    public Response getAsJson(
        @HeaderParam("Authorization") String authKey,
        @HeaderParam("X-device-eui") String inHeaderEui,
        String jsonString
    ) {
        try {
            if (
                authorizationRequired && (null == authKey || authKey.isBlank())
            ) {
                return Response.status(Status.UNAUTHORIZED)
                    .entity("no authorization header fond")
                    .build();
            }
            sendDataToService(
                authKey + SEPARATOR + inHeaderEui + SEPARATOR + jsonString
            );
            return Response.ok("OK").build();
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            e.printStackTrace();
            return Response.status(Status.INTERNAL_SERVER_ERROR)
                .entity("error while processing the data")
                .build();
        }
    }

    private void sendDataToService(String dataMessage) {
        bus.send("agent-no-response", dataMessage);
        LOG.debug("sent");
    }
}
