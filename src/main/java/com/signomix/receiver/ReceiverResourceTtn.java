package com.signomix.receiver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
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
import com.signomix.common.iot.ttn3.Decoder;
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

    public void onApplicationStart(@Observes StartupEvent event) {
        try {
            bus.registerCodec(new IotDataMessageCodec());
        } catch (Exception e) {

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
    public Response getAsJson(@HeaderParam("Authorization") String authKey, String jsonString) {
        // LOG.debug("input: " + dataObject.toString());
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        Decoder decoder = new Decoder();
        TtnData3 dataObject = decoder.decode(jsonString);
        IotData2 iotData = transform(dataObject, authKey, authorizationRequired);
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

    private IotData2 transform(TtnData3 dataObject, String authKey, boolean authRequired) {
        LOG.info("transform "+authKey+" "+authRequired);
        IotData2 data = new IotData2();
        data.dev_eui = dataObject.deviceEui;
        data.gateway_eui = null;
        data.timestamp = "" + dataObject.getTimestamp();
        data.clientname = "";
        data.authKey = authKey;
        data.authRequired = authRequired;
        // TODO:
        data.payload_fields = new ArrayList<>();
        HashMap pfMap = dataObject.getPayloadFields();
        // Data channel names should be lowercase. We can fix user mistakes here.
        HashMap<String, Object> tempMap;
        Iterator<String> it = pfMap.keySet().iterator();
        String key;
        while (it.hasNext()) {
            tempMap = new HashMap<>();
            key = it.next();
            tempMap.put("name", key.toLowerCase());
            try {
                tempMap.put("value", (Double) pfMap.get(key));
            } catch (ClassCastException ex) {
                try {
                    tempMap.put("value", (Long) pfMap.get(key));
                } catch (ClassCastException ex2) {
                    tempMap.put("value", (String) pfMap.get(key));
                }
            }
            data.payload_fields.add(tempMap);
        }
        data.normalize();
        data.setTimestampUTC();
        return data;
    }

}