package com.signomix.receiver;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.signomix.common.iot.generic.IotData2;
import com.signomix.common.iot.generic.IotDto;
import com.signomix.common.iot.ttn3.TtnData3;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.mutiny.core.eventbus.EventBus;

@Path("/api")
@ApplicationScoped
public class ReceiverResourceTtn {
    private static final Logger LOG = Logger.getLogger(ReceiverResourceGeneric.class);

    @Inject
    EventBus bus;

    @Inject
    ReceiverService service;

    @ConfigProperty(name = "device.authorization.required")
    Boolean authorizationRequired;

    @ConfigProperty(name = "device.eui.header.first")
    Boolean euiHeaderFirst;

    public void onApplicationStart(@Observes StartupEvent event) {
        bus.registerCodec(new IotDataMessageCodec());
    }

    @Path("/ttn3")
    @OPTIONS
    public String sendOKString() {
        return "OK";
    }

    @Path("/ttn3")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response getAsJson(@HeaderParam("Authorization") String authKey, TtnData3 dataObject) {
        LOG.debug("input: " + dataObject.toString());
        if (null == authKey || authKey.isBlank()) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        IotData2 iotData = transform(dataObject);
        if (null == iotData) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        } else {
            send(iotData);
        }
        return Response.ok("OK").build();
    }

    private void send(IotData2 iotData) {
        IotDataMessageCodec iotDataCodec = new IotDataMessageCodec();
        DeliveryOptions options = new DeliveryOptions().setCodecName(iotDataCodec.name());
        bus.send("ttndata-no-response", iotData, options);
        LOG.debug("sent");
    }

    private IotData2 transform(TtnData3 dataObject) {
        IotData2 data = new IotData2();
            data.dev_eui = dataObject.deviceEui;
        data.gateway_eui = null;
        data.timestamp = "" + dataObject.getTimestamp();
        data.clientname = "";
        //TODO:
        //data.payload_fields = dataObject.getPayloadFields();
        data.normalize();
        data.setTimestampUTC();
        return data;
    }

}