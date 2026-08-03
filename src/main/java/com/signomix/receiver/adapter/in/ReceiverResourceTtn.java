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
public class ReceiverResourceTtn {

    @Inject
    Logger LOG;

    @Inject
    EventBus bus;

    @Inject
    ReceiverService service;

    @ConfigProperty(name = "device.authorization.required")
    Boolean authorizationRequired;

    public void onApplicationStart(@Observes StartupEvent event) {
        try {
            bus.registerCodec(new IotDataMessageCodec());
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    @Path("/receiver/ttn3")
    @OPTIONS
    public String sendOKString() {
        return "OK";
    }

    @Path("/receiver/ttn3/up")
    @POST
    @Produces(MediaType.TEXT_PLAIN)
    public Response getAsJson(
        @HeaderParam("Authorization") String authKey,
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
            sendDataToService(authKey + "@" + jsonString);
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
        bus.<String>requestAndForget("ttndata3-no-response", dataMessage);
        LOG.debug("sent");
    }
}
