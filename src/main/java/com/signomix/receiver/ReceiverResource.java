package com.signomix.receiver;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.json.JsonObject;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.signomix.common.iot.generic.IotData2;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.mutiny.core.eventbus.EventBus;

@Path("/api")
@ApplicationScoped
public class ReceiverResource {
    private static final Logger LOG = Logger.getLogger(ReceiverResource.class);

    @Inject
    EventBus bus;

    @Inject
    ReceiverService service;

    @ConfigProperty(name = "device.authorization.required")
    Boolean authorizationRequired;

    @ConfigProperty(name = "device.eui.header.first")
    Boolean euiHeaderFirst;

    @ConfigProperty(name = "parser.class.name")
    String parserClassName;

    public void onApplicationStart(@Observes StartupEvent event) {
        bus.registerCodec(new IotDataMessageCodec());
    }

    @Path("/receiver/in")
    @OPTIONS
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    public String sendOKString() {
        return "OK";
    }

    @Path("/receiver/in")
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    public Response getAsForm(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, MultivaluedMap<String, String> form) {
        Iterator<String> it = form.keySet().iterator();
        String key;
        while (it.hasNext()) {
            key = it.next();
            LOG.info(key + "=" + form.getFirst(key));
        }
        IotData2 iotData = null;
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        } else {
            iotData = parseIotData(inHeaderEui, form);
        }
        return Response.ok("OK").build();
    }

    @Path("/receiver/in")
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response getAsText(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, String input) {
        LOG.info("input: " + input);
        IotData2 iotData = null;
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        } else {
            iotData = parseIotData(inHeaderEui, input);
        }
        IotDataMessageCodec iotDataCodec = new IotDataMessageCodec();
        DeliveryOptions options = new DeliveryOptions().setCodecName(iotDataCodec.name());
        bus.send("iotdata-no-response", iotData, options);
        LOG.info("sent");
        return Response.ok("OK").build();
    }

    private IotData2 parseIotData(String eui, String input) {
        IotData2 data=new IotData2();
        data.dev_eui=eui;
        HashMap<String,Object> options=new HashMap<>();
        //options.put("eui", eui);
        //options.put("euiInHeader", ""+euiHeaderFirst);
        PayloadParserIface parser;
        try {
            Class clazz = Class.forName(parserClassName);
            parser = (PayloadParserIface) clazz.getDeclaredConstructor().newInstance();
            data.payload_fields=(ArrayList)parser.parse(input,options);
            if(!euiHeaderFirst || (null==data.dev_eui || data.dev_eui.isEmpty())){
                data.dev_eui=getEuiParamValue(data.payload_fields);
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }

    private String getEuiParamValue(ArrayList<Map> params){
        Map<String, String> map;
        for(int i=0; i<params.size(); i++){
            map=params.get(i);
            if("eui".equals(map.get("name"))){
                return map.get("value");
            }
        }
        return null;
    }

    private IotData2 parseIotData(String eui, MultivaluedMap<String, String> form) {
        IotData2 data = new IotData2();
        data.dev_eui = eui;
        /*
         * data.dev_eui = null;
         * data.timestamp = "" + System.currentTimeMillis();
         * data.payload_fields = new ArrayList<>();
         * HashMap<String, String> map;
         * for (Map.Entry<String, Object> entry : parameters.entrySet()) {
         * String key = entry.getKey();
         * String value = (String) entry.getValue();
         * if ("eui".equalsIgnoreCase(key)) {
         * data.dev_eui = value;
         * System.out.println("dev_eui:" + data.dev_eui);
         * } else if ("timestamp".equalsIgnoreCase(key)) {
         * data.timestamp = value;
         * } else if ("authkey".equalsIgnoreCase(key)) {
         * data.authKey = value;
         * } else if ("clienttitle".equalsIgnoreCase(key)) {
         * data.clientname = value;
         * } else if ("payload".equalsIgnoreCase(key)) {
         * data.payload = value;
         * } else {
         * map = new HashMap<>();
         * map.put("name", key);
         * map.put("value", value);
         * data.payload_fields.add(map);
         * System.out.println(key + ":" + value);
         * }
         * System.out.println("timestamp:" + data.timestamp);
         * }
         * if (null == data.dev_eui || (data.payload_fields.isEmpty() &&
         * null==data.payload)) {
         * System.out.println("ERROR: " + data.dev_eui + "," + data.payload_fields);
         * return null;
         * }
         * data.normalize();
         */
        return data;
    }

    private IotData2 parseJson(String eui, JsonObject o) {
        IotData2 data = new IotData2();
        data.dev_eui = eui;
        // TODO
        /*
         * Uplink iotData = new Uplink();
         * iotData.setAdr((boolean) o.get("adr"));
         * iotData.setApplicationID((String) o.get("applicationID"));
         * iotData.setApplicationName((String) o.get("applicationName"));
         * iotData.setData((String) o.get("data"));
         * iotData.setDevEUI((String) o.get("devEUI"));
         * iotData.setDeviceName((String) o.get("deviceName"));
         * iotData.setfCnt((long) o.get("fCnt"));
         * iotData.setfPort((long) o.get("fPort"));
         * 
         * //tx
         * JsonObject txObj = (JsonObject) o.get("txInfo");
         * TxInfo txInfo = new TxInfo();
         * txInfo.setDr((long) txObj.get("dr"));
         * txInfo.setFrequency((long) txObj.get("frequency"));
         * iotData.setTxInfo(txInfo);
         * 
         * //rx
         * ArrayList<RxInfo> rxList = new ArrayList<>();
         * //List<JsonObject> objList = (List) o.get("rxInfo");
         * Object[] jo = (Object[]) o.get("rxInfo");
         * JsonObject rxObj, locObj;
         * RxInfo rxInfo;
         * Location loc;
         * 
         * for (int i = 0; i < jo.length; i++) {
         * rxObj = (JsonObject) jo[i];
         * rxInfo = new RxInfo();
         * rxInfo.setGatewayID((String) rxObj.get("gatewayID"));
         * rxInfo.setUplinkID((String) rxObj.get("uplinkID"));
         * rxInfo.setName((String) rxObj.get("name"));
         * rxInfo.setRssi((long) rxObj.get("rssi"));
         * rxInfo.setLoRaSNR((long) rxObj.get("loRaSNR"));
         * locObj = (JsonObject) rxObj.get("location");
         * loc = new Location();
         * loc.setLatitude((Double) locObj.get("latitude"));
         * loc.setLongitude((Double) locObj.get("longitude"));
         * loc.setAltitude((long) locObj.get("altitude"));
         * rxInfo.setLocation(loc);
         * rxList.add(rxInfo);
         * }
         * iotData.setRxInfo(rxList);
         * 
         * //data
         * JsonObject data = (JsonObject) o.get("object");
         * JsonObject dataMap;
         * JsonObject dataFieldsMap;
         * Iterator it = data.keySet().iterator();
         * Iterator it2, it3;
         * String dataName;
         * String dataIndex, dataField;
         * Object dataValue;
         * while (it.hasNext()) {
         * dataName = (String) it.next();
         * dataMap = (JsonObject) data.get(dataName);
         * it2 = dataMap.keySet().iterator();
         * while (it2.hasNext()) {
         * dataIndex = (String) it2.next();
         * dataValue = dataMap.get(dataIndex);
         * if (dataValue instanceof Double) {
         * iotData.addField(dataName + "_" + dataIndex, (Double) dataValue);
         * } else {
         * dataFieldsMap = (JsonObject) dataValue;
         * it3 = dataFieldsMap.keySet().iterator();
         * while (it3.hasNext()) {
         * dataField = (String) it3.next();
         * dataValue = dataFieldsMap.get(dataField);
         * iotData.addField(dataName + "_" + dataIndex + "_" + dataField, (Double)
         * dataValue);
         * }
         * }
         * }
         * }
         * return iotData;
         */
        return data;
    }

}