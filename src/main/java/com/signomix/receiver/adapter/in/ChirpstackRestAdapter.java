package com.signomix.receiver.adapter.in;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.receiver.IotDataMessageCodec;
import com.signomix.receiver.domain.uplink.ChirpstackUplink;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

/**
 * Interfejs odbierający dane z LNS ChirpStack
 */
// @InboundAdapter
@Path("/api")
@ApplicationScoped
public class ChirpstackRestAdapter {
    
    @Inject
    Logger LOG;

    @ConfigProperty(name = "signomix.receiver.exception.api.param.missing")
    String missingParameterException;
    @ConfigProperty(name = "device.authorization.required")
    Boolean authorizationRequired;
    @ConfigProperty(name = "receiver.api.mode")
    String apiMode;

    @Inject
    EventBus bus;

    public void onApplicationStart(@Observes StartupEvent event) {
        try {
            bus.registerCodec(new IotDataMessageCodec());
        } catch (Exception e) {

        }
    }
    
    /*
     * @Inject
     * ChirpstackEventPort chirpstackPort;
     */

    @POST
    @Path("/receiver/chirpstack")
    @Transactional
    @Produces(MediaType.TEXT_PLAIN)
    public Response handle(@HeaderParam("Authorization") String authKey, String event,
            @QueryParam("event") String eventType)/*  throws ServiceException */ {
        if (null == eventType) {
            //throw new ServiceException(missingParameterException);
            return Response.status(Status.BAD_REQUEST).entity("event parammeter missing").build();
        }
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        switch (eventType) {
            case "up":
                IotData2 iotData=handleUplink(event, authKey);
                if (null == iotData) {
                    return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
                } else {
                    send(iotData);
                }
                break;
            case "join":
                // handleJoin(event);
                break;
            default:
                System.out.println(eventType);
                break;
        }
        return Response.ok().build();
    }

    private void send(IotData2 iotData) {
        IotDataMessageCodec iotDataCodec = new IotDataMessageCodec();
        DeliveryOptions options = new DeliveryOptions().setCodecName(iotDataCodec.name());
        bus.send("chirpstackdata-no-response", iotData, options);
        LOG.debug("sent");
    }

    private IotData2 handleUplink(String event, String authKey) {
        ObjectMapper mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        LOG.info(event);
        ChirpstackUplink uplink;
        IotData2 iotData=null;
        try {
            uplink = mapper.readValue(event, ChirpstackUplink.class);
            // chirpstackPort.processUplink(uplink);
            iotData = transform(uplink, authKey, authorizationRequired);
            
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return iotData;
    }

    private IotData2 transform(ChirpstackUplink uplink, String authKey, boolean authorizationRequired) {
        IotData2 data = new IotData2();
        LOG.info("transform " + authKey + " " + authorizationRequired);
        data.dev_eui = uplink.deviceinfo.devEui;
        data.gateway_eui = null;
        if ("dev".equalsIgnoreCase(apiMode) || "test".equalsIgnoreCase(apiMode)) {
            DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
            data.timestamp = df.format(LocalDateTime.now());
            data.timestamp=data.timestamp+"Z";
            System.out.println(data.timestamp);
        } else {
            data.timestamp = uplink.time;
        }
        data.time=data.timestamp;
        data.clientname = "";
        data.authKey = authKey;
        data.authRequired = authorizationRequired;
        data.payload_fields = new ArrayList<>();
        HashMap pfMap = null;
        if (null != uplink.objectJSON && !uplink.objectJSON.isEmpty()) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                pfMap = mapper.readValue(uplink.objectJSON, HashMap.class);
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        if (null != pfMap) {
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
                        try {
                            tempMap.put("value", (Integer) pfMap.get(key));
                        } catch (ClassCastException ex3) {
                            tempMap.put("value", (String) pfMap.get(key));
                        }
                    }
                }
                data.payload_fields.add(tempMap);
            }
        }
        /*
         * if ("dev".equalsIgnoreCase(apiMode) || "test".equalsIgnoreCase(apiMode)) {
         * data.timestampUTC=new Timestamp(System.currentTimeMillis());
         * } else {
         * DateTimeFormatter df =
         * DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSSXXX");
         * data.timestampUTC = new Timestamp(1000*OffsetDateTime.parse(uplink.time,
         * df).toEpochSecond());
         * }
         */
        data.normalize();
        data.setTimestampUTC();
        return data;
    }

    /*
     * private void handleJoin(String event) {
     * ObjectMapper mapper = new ObjectMapper()
     * .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
     * 
     * Join join;
     * try {
     * join = mapper.readValue(event, Join.class);
     * DeviceEvent deviceEvent=new DeviceEvent();
     * deviceEvent.deviceEui=join.deviceinfo.devEui;
     * deviceEvent.eventType="join";
     * deviceEvent.createdAt=new Date(); //TODO
     * deviceEvent.jsonPayload=event;
     * System.out.println(join.deviceinfo.tags.get("key"));
     * //chirpstackPort.processEvent(deviceEvent);
     * } catch (JsonProcessingException e) {
     * e.printStackTrace();
     * }
     * 
     * }
     */
}
