package com.signomix.receiver;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.signomix.common.api.PayloadParserIface;
import com.signomix.common.api.ResponseTransformerIface;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.common.iot.generic.IotDto;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.mutiny.core.eventbus.EventBus;

@Path("/api")
@ApplicationScoped
public class ReceiverResourceGeneric {
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

    @Path("/receiver/in")
    @OPTIONS
    public String sendOKString() {
        return "OK";
    }

    @Path("/receiver/io")
    @OPTIONS
    public String sendOKString2() {
        return "OK";
    }

    @Path("/receiver/in")
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    public Response getAsForm(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, MultivaluedMap<String, String> form) {
        LOG.debug("form received");
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        IotData2 iotData = parseFormData(inHeaderEui, authorizationRequired, form);
        if (null == iotData) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        } else {
            send(iotData);
        }
        if (null != iotData.clientname && !iotData.clientname.isEmpty()) {
            return Response.ok(buildResultData(true, true, iotData.clientname, "Data saved."))
                    .header("Content-type", "text/html").build();
        } else {
            return Response.ok("OK").build();
        }
    }

    @Path("/receiver/in")
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response getAsText(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, String input) {
        LOG.debug("input: " + input);
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        Device device = service.getDevice(inHeaderEui);
        if (null == device) {
            LOG.warn("unknown device " + inHeaderEui);
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        }
        IotData2 iotData = parseTextData(device, authorizationRequired, input);
        if (null == iotData) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        } else {
            send(iotData);
        }
        return Response.ok("OK").build();
    }

    @Path("/receiver/io")
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response processText(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, String input) {
        LOG.debug("input: " + input);
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        Device device = service.getDevice(inHeaderEui);
        if (null == device) {
            LOG.warn("unknown device " + inHeaderEui);
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        }
        IotData2 iotData = parseTextData(device, authorizationRequired, input);
        if (null == iotData) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        } else {
            String standardResult = service.processDataAndReturnResponse(iotData);
            String result = runDedicatedResponder(device, standardResult);
            return Response.ok(result).build();
        }
    }

    @Path("/receiver/io")
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    public Response processForm(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, MultivaluedMap<String, String> form) {
        LOG.debug("form processing");
        String result;
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        IotData2 iotData = parseFormData(inHeaderEui, authorizationRequired, form);
        if (null == iotData) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        } else {
            result = service.processDataAndReturnResponse(iotData);
        }
        if (null != iotData.clientname && !iotData.clientname.isEmpty()) {
            return Response.ok(buildResultData(true, true, iotData.clientname, "Data saved."))
                    .header("Content-type", "text/html").build();
        } else {
            return Response.ok(result).build();
        }
    }

    @Path("/receiver/in")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response getAsJson(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, IotDto dataObject) {
        LOG.debug("input: " + dataObject.toString());
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        IotData2 iotData = parseJson(inHeaderEui, authorizationRequired, dataObject);
        if (null == iotData) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        } else {
            send(iotData);
        }
        return Response.ok("OK").build();
    }

    @Path("/receiver/io")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response processJson(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, IotDto dataObject) {
        LOG.debug("input: " + dataObject.toString());
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        IotData2 iotData = parseJson(inHeaderEui, authorizationRequired, dataObject);
        if (null == iotData) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        } else {
            String result = service.processDataAndReturnResponse(iotData);
            return Response.ok(result).build();
        }
    }

    private void send(IotData2 iotData) {
        IotDataMessageCodec iotDataCodec = new IotDataMessageCodec();
        DeliveryOptions options = new DeliveryOptions().setCodecName(iotDataCodec.name());
        bus.send("iotdata-no-response", iotData, options);
        LOG.debug("sent");
    }

    private IotData2 runDedicatedParser(Device device, String input) {
        // put your specific code here
        if (null == device) {
            return null;
        }
        HashMap<String, Object> appConfig = device.getApplicationConfig();
        IotData2 data = new IotData2();
        data.dev_eui = device.getEUI();
        PayloadParserIface parser;
        try {
            Class clazz = Class.forName((String) appConfig.get("parser"));
            parser = (PayloadParserIface) clazz.getDeclaredConstructor().newInstance();
            data.payload_fields = (ArrayList) parser.parse(input, appConfig);
            data.payload_fields.forEach((m) -> {
                LOG.debug(m);
            });
            if (!euiHeaderFirst || (null == data.dev_eui || data.dev_eui.isEmpty())) {
                data.dev_eui = getEuiParamValue(data.payload_fields);
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
                    e.printStackTrace();
            LOG.error(e.getMessage());
            return null;
        }
        data.setTimestampUTC();
        return data;
    }

    private String runDedicatedResponder(Device device, String originalResponse) {
        if (null == device) {
            return null;
        }
        LOG.debug("Command to send: "+originalResponse);
        HashMap<String, Object> appConfig = device.getApplicationConfig();
        ResponseTransformerIface formatter;
        String result = null;
        try {
            Class clazz = Class.forName((String) appConfig.get("formatter"));
            formatter = (ResponseTransformerIface) clazz.getDeclaredConstructor().newInstance();
            result = formatter.transform(originalResponse, appConfig);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            LOG.error(e.getMessage());
            return null;
        }
        LOG.debug("response to transform:"+originalResponse+" size:"+originalResponse.length());
        LOG.debug("response transformed:"+result);
        return result;
    }

    private IotData2 parseTextData(Device device, boolean authRequired, String input) {
        IotData2 data = runDedicatedParser(device, input);
        if (null != data) {
            return data;
        }
        data = new IotData2();
        data.dev_eui = device.getEUI();
        HashMap<String, Object> options = new HashMap<>();
        // options.put("eui", eui);
        // options.put("euiInHeader", ""+euiHeaderFirst);
        PayloadParserIface parser = new com.signomix.receiver.PayloadParser();
        data.payload_fields = (ArrayList) parser.parse(input, options);
        if (!euiHeaderFirst || (null == data.dev_eui || data.dev_eui.isEmpty())) {
            data.dev_eui = getEuiParamValue(data.payload_fields);
        }
        data.normalize();
        data.setTimestampUTC();
        data.authRequired = authRequired;
        return data;
    }

    private String getEuiParamValue(ArrayList<Map> params) {
        Map<String, String> map;
        for (int i = 0; i < params.size(); i++) {
            map = params.get(i);
            if ("eui".equals(map.get("name"))) {
                return map.get("value");
            }
        }
        return null;
    }

    private IotData2 parseFormData(String eui, boolean authRequired, MultivaluedMap<String, String> form) {
        IotData2 data = new IotData2();
        data.dev_eui = eui;
        data.payload_fields = new ArrayList<>();
        HashMap<String, String> map;
        Iterator<String> it = form.keySet().iterator();
        String key;
        String value;
        while (it.hasNext()) {
            key = it.next();
            value = form.getFirst(key);
            LOG.debug(key + "=" + value);
            if ("eui".equalsIgnoreCase(key)) {
                data.dev_eui = value;
            } else if ("timestamp".equalsIgnoreCase(key)) {
                data.timestamp = value;
            } else if ("authkey".equalsIgnoreCase(key)) {
                data.authKey = value;
            } else if ("clienttitle".equalsIgnoreCase(key)) {
                data.clientname = value;
            } else if ("payload".equalsIgnoreCase(key)) {
                data.payload = value;
            } else {
                map = new HashMap<>();
                map.put("name", key);
                map.put("value", value);
                data.payload_fields.add(map);
            }
        }
        if (null == data.dev_eui || (data.payload_fields.isEmpty() && null == data.payload)) {
            LOG.warn("ERROR: " + data.dev_eui + "," + data.payload_fields);
            return null;
        }
        data.normalize();
        data.setTimestampUTC();
        data.authRequired = authRequired;
        return data;
    }

    private IotData2 parseJson(String eui, boolean authRequired, IotDto dataObject) {
        IotData2 data = new IotData2();
        data.dev_eui = eui;
        if (null != dataObject.dev_eui && !dataObject.dev_eui.isEmpty()) {
            data.dev_eui = dataObject.dev_eui;
        }
        data.gateway_eui = dataObject.gateway_eui;
        data.timestamp = "" + dataObject.timestamp;
        data.clientname = dataObject.clientname;
        data.payload_fields = dataObject.payload_fields;
        data.normalize();
        data.setTimestampUTC();
        data.authRequired = authRequired;
        return data;
    }

    String buildResultData(boolean html, boolean isSuccess, String title, String text) {
        if (!html) {
            return text;
        }
        String err = isSuccess ? "" : "ERROR<br>";
        StringBuilder sb = new StringBuilder();
        sb.append("<html><body style='text-align: center;'><h1>")
                .append(title)
                .append("</h1><p>")
                .append(err)
                .append(text)
                .append("</p><button type='button' onclick='window.history.go(-1); return false;'>")
                .append("OK")
                .append("</button></body></html>");
        return sb.toString();
    }

}