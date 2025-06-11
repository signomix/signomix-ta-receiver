package com.signomix.receiver.adapter.in;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.jboss.resteasy.annotations.providers.multipart.MultipartForm;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import com.signomix.common.api.PayloadParserIface;
import com.signomix.common.api.ResponseTransformerIface;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.common.iot.generic.IotDto;
import com.signomix.receiver.BulkLoaderResult;
import com.signomix.receiver.IotDataMessageCodec;
import com.signomix.receiver.ReceiverService;

import io.quarkus.runtime.StartupEvent;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import jakarta.ws.rs.core.Response.Status;

@Path("/api")
@ApplicationScoped
public class ReceiverResourceGeneric {

    @Inject
    Logger LOG;

    @Inject
    EventBus bus;

    @Inject
    ReceiverService service;

    @ConfigProperty(name = "device.authorization.required")
    Boolean authorizationRequired;

    @ConfigProperty(name = "device.eui.header.required")
    Boolean euiHeaderFirst;

    public void onApplicationStart(@Observes StartupEvent event) {
        try {
            bus.registerCodec(new IotDataMessageCodec());
        } catch (Exception e) {
        }
    }

    /*
     * @Path("/receiver/in")
     * 
     * @OPTIONS
     * public Response sendOKString() {
     * return Response.ok().build();
     * }
     * 
     * @Path("/receiver/io")
     * 
     * @OPTIONS
     * public Response sendOKString2() {
     * return Response.ok().build();
     * }
     */

    @Path("/receiver/io")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response processJson(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, IotDto dataObject) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("processJson");
            LOG.debug("input: " + dataObject.toString());
        }
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        // When eui in request header
        // Then device can be checked
        Device device = null;
        if (euiHeaderFirst) {
            device = service.getDevice(inHeaderEui);
            if (null == device) {
                LOG.warn("unknown device " + inHeaderEui);
                return Response.status(Status.NOT_FOUND).entity("device not found").build();
            }
            if (!device.isActive()) {
                return Response.status(Status.NOT_FOUND).entity("device is not active").build();
            }
        }
        try {
            IotData2 iotData = parseJson(inHeaderEui, authorizationRequired, authKey, dataObject);
            if (null == iotData) {
                return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
            } else {
                if (!euiHeaderFirst) {
                    device = service.getDevice(iotData.dev_eui);
                    if (null == device) {
                        LOG.warn("unknown device " + iotData.dev_eui);
                        return Response.status(Status.NOT_FOUND).entity("device not found").build();
                    }
                    if (!device.isActive()) {
                        return Response.status(Status.NOT_FOUND).entity("device is not active").build();
                    }
                }
                String result = service.processDataAndReturnResponse(iotData);
                if (null == result) {
                    return Response.status(Status.NOT_FOUND).entity("device not found or no access rights").build();
                } else if (result.startsWith("error")) {
                    return Response.status(Status.BAD_REQUEST).entity(result).build();
                }
                // request from html app
                // TODO: describe
                if (null != iotData.clientname && !iotData.clientname.isEmpty()) {
                    return Response.ok(buildResultData(true, true, iotData.clientname, "Data saved."))
                            .header("Content-type", "text/html").build();
                }
                // return Response.ok(result).build();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("RESULT BEFORE TRANSFORMER:" + result);
                }
                String transformedResult = runDedicatedResponder(device, result);
                Map<String, String> headers = getDedicatedResponderHeaders(device, result);
                ResponseBuilder rb = Response.ok(transformedResult);
                headers.keySet().forEach(key -> {
                    rb.header(key, headers.get(key));
                });
                return rb.build();
            }
        } catch (Exception e) {
            return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
        }
    }

    @Path("/receiver/io")
    @POST
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.TEXT_PLAIN)
    public Response processText(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui,
            @HeaderParam("X-data-separator") String separator,
            String input) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("processText");
            LOG.debug("input: " + input);
        }
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        Device device = null;
        if (euiHeaderFirst) {
            device = service.getDevice(inHeaderEui);
            if (null == device) {
                LOG.warn("unknown device " + inHeaderEui);
                return Response.status(Status.NOT_FOUND).entity("device not found").build();
            }
            if (!device.isActive()) {
                return Response.status(Status.NOT_FOUND).entity("device is not active").build();
            }
        }
        IotData2 iotData = parseTextData(device, authorizationRequired, input, authKey, separator);
        if (null == iotData) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        } else {
            try {
                if (!euiHeaderFirst) {
                    device = service.getDevice(iotData.dev_eui);
                    if (null == device) {
                        LOG.warn("unknown device " + iotData.dev_eui);
                        return Response.status(Status.NOT_FOUND).entity("device not found").build();
                    }
                    if (!device.isActive()) {
                        return Response.status(Status.NOT_FOUND).entity("device is not active").build();
                    }
                }
                String result = service.processDataAndReturnResponse(iotData);
                if (null == result) {
                    return Response.status(Status.NOT_FOUND).entity("device not found or no access rights").build();
                } else if (result.startsWith("error")) {
                    return Response.status(Status.BAD_REQUEST).entity(result).build();
                }
                // request from html app
                // TODO: describe
                if (null != iotData.clientname && !iotData.clientname.isEmpty()) {
                    return Response.ok(buildResultData(true, true, iotData.clientname, "Data saved."))
                            .header("Content-type", "text/html").build();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("RESULT BEFORE TRANSFORMER:" + result);
                }
                String transformedResult = runDedicatedResponder(device, result);
                Map<String, String> headers = getDedicatedResponderHeaders(device, result);
                ResponseBuilder rb = Response.ok(transformedResult);
                headers.keySet().forEach(key -> {
                    rb.header(key, headers.get(key));
                });
                return rb.build();
            } catch (Exception e) {
                return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
            }
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
        // When eui in request header
        // Then device can be checked
        Device device = null;
        if (euiHeaderFirst) {
            device = service.getDevice(inHeaderEui);
            if (null == device) {
                if (LOG.isDebugEnabled()) {
                    LOG.warn("unknown device " + inHeaderEui);
                }
                return Response.status(Status.NOT_FOUND).entity("device not found").build();
            }
            if (!device.isActive()) {
                return Response.status(Status.NOT_FOUND).entity("device is not active").build();
            }
        }
        IotData2 iotData = parseFormData(inHeaderEui, authorizationRequired, form, authKey);
        if (!euiHeaderFirst) {
            device = service.getDevice(iotData.dev_eui);
            if (null == device) {
                LOG.warn("unknown device " + iotData.dev_eui);
                return Response.status(Status.NOT_FOUND).entity("device not found").build();
            }
            if (!device.isActive()) {
                return Response.status(Status.NOT_FOUND).entity("device is not active").build();
            }
        }
        if (null == iotData) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        } else {
            try {
                result = service.processDataAndReturnResponse(iotData);
                // request from html app
                // TODO: describe
                if (null != iotData.clientname && !iotData.clientname.isEmpty()) {
                    return Response.ok(buildResultData(true, true, iotData.clientname, "Data saved."))
                            .header("Content-type", "text/html").build();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("RESULT BEFORE TRANSFORMER:" + result);
                }
                String transformedResult = runDedicatedResponder(device, result);
                Map<String, String> headers = getDedicatedResponderHeaders(device, result);
                ResponseBuilder rb = Response.ok(transformedResult);
                headers.keySet().forEach(key -> {
                    rb.header(key, headers.get(key));
                });
                return rb.build();
                // return Response.ok(result).build();
            } catch (Exception e) {
                return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
            }
        }

    }

    @Path("/receiver/in")
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    public Response getAsForm(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, MultivaluedMap<String, String> form) {
        // LOG.info("form received from eui "+inHeaderEui);
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        // When eui in request header
        // Then device can be checked
        if (euiHeaderFirst) {
            Device device = service.getDevice(inHeaderEui == null ? "" : inHeaderEui);
            if (null == device) {
                LOG.warn("unknown device " + inHeaderEui);
                return Response.status(Status.BAD_REQUEST).entity("device not registered").build();
            }
            if (!device.isActive()) {
                return Response.status(Status.NOT_FOUND).entity("device is not active").build();
            }
        }
        IotData2 iotData = parseFormData(inHeaderEui, authorizationRequired, form, authKey);
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
            @HeaderParam("X-device-eui") String inHeaderEui,
            @HeaderParam("X-data-separator") String separator,
            String input) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("input: " + input);
        }
        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header found").build();
        }
        // In this case device EUI mus be in request header
        Device device = service.getDevice(inHeaderEui);
        if (null == device) {
            LOG.warn("unknown device " + inHeaderEui);
            return Response.status(Status.BAD_REQUEST).entity("device not registered").build();
        }
        if (!device.isActive()) {
            return Response.status(Status.NOT_FOUND).entity("device is not active").build();
        }
        IotData2 iotData = parseTextData(device, authorizationRequired, input, authKey, separator);
        if (null == iotData) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        } else {
            send(iotData);
        }
        return Response.ok("OK").build();
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
        // When eui in request header
        // Then device can be checked
        if (euiHeaderFirst) {
            Device device = service.getDevice(inHeaderEui);
            if (null == device) {
                LOG.warn("unknown device " + inHeaderEui);
                return Response.status(Status.BAD_REQUEST).entity("device not registered").build();
            }
            if (!device.isActive()) {
                return Response.status(Status.NOT_FOUND).entity("device is not active").build();
            }
        }
        try {
            IotData2 iotData = parseJson(inHeaderEui, authorizationRequired, authKey, dataObject);
            if (null == iotData) {
                return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
            } else {
                send(iotData);
            }
        } catch (Exception e) {
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        }
        return Response.ok("OK").build();
    }

    @POST
    @Path("/receiver/bulk")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response fileUpload(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, @MultipartForm MultipartFormDataInput input) {

        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        // In this case device EUI mus be in request header
        Device device = service.getDevice(inHeaderEui);
        if (null == device) {
            LOG.warn("unknown device " + inHeaderEui);
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        }
        if (!device.isActive()) {
            return Response.status(Status.NOT_FOUND).entity("device is not active").build();
        }
        BulkLoaderResult result = service.processCsv(device, input, true);
        return Response.ok().entity(result).build();
    }

    /**
     * Process a batch of data from an edge device. The edge device is identified by
     * the EUI in the header.
     * The data is in CSV format.
     * 
     * @param authKey
     * @param inHeaderEui
     * @param input
     * @return
     */
    @POST
    @Path("/receiver/edge")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response processBatch(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, String input) {

        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        // The device is an Signomix Edge service.
        Device device = service.getDevice(inHeaderEui);
        if (null == device) {
            LOG.warn("unknown device " + inHeaderEui);
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        }
        if (!device.isActive()) {
            return Response.status(Status.NOT_FOUND).entity("device is not active").build();
        }
        BulkLoaderResult result = service.processCsvString(device, input);
        return Response.ok().entity(result).build();
    }

    @POST
    @Path("/receiver/edge")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces(MediaType.APPLICATION_JSON)
    public Response processBatchFile(@HeaderParam("Authorization") String authKey,
            @HeaderParam("X-device-eui") String inHeaderEui, @MultipartForm MultipartFormDataInput input) {

        if (authorizationRequired && (null == authKey || authKey.isBlank())) {
            return Response.status(Status.UNAUTHORIZED).entity("no authorization header fond").build();
        }
        // The device is an Signomix Edge service.
        Device device = service.getDevice(inHeaderEui);
        if (null == device) {
            LOG.warn("unknown device " + inHeaderEui);
            return Response.status(Status.BAD_REQUEST).entity("error while reading the data").build();
        }
        if (!device.isActive()) {
            return Response.status(Status.NOT_FOUND).entity("device is not active").build();
        }
        BulkLoaderResult result = service.processCsv(device, input, false);
        return Response.ok().entity(result).build();
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
        long systemTimestamp = System.currentTimeMillis();
        HashMap<String, Object> devConfig = device.getConfigurationMap(); // device.getApplicationConfig();
        String className = (String) devConfig.get("parser");
        if (null == className || className.isEmpty()) {
            return null;
        }
        // to expose the device EUI to a parser
        devConfig.put("dev_eui", device.getEUI());
        IotData2 data = new IotData2(systemTimestamp);
        data.dev_eui = device.getEUI();
        PayloadParserIface parser;
        try {
            Class clazz = Class.forName(className);
            parser = (PayloadParserIface) clazz.getDeclaredConstructor().newInstance();
            data.payload_fields = (ArrayList) parser.parse(input, devConfig);
            if (LOG.isDebugEnabled()) {
                data.payload_fields.forEach((m) -> {
                    LOG.debug(m);
                });
            }
            if (!euiHeaderFirst || (null == data.dev_eui || data.dev_eui.isEmpty())) {
                data.dev_eui = getEuiParamValue(data.payload_fields);
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            return null;
        }
        //data.setTimestampUTC(systemTimestamp);
        return data;
    }

    private String runDedicatedResponder(Device device, String originalResponse) throws Exception {
        if (null == device) {
            return null;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Command to send: " + originalResponse);
        }
        HashMap<String, Object> devConfig = device.getConfigurationMap();
        devConfig.put("dev_eui", device.getEUI());
        ResponseTransformerIface formatter;
        String result = originalResponse;
        String className = (String) devConfig.get("formatter");
        if (null == className || className.isEmpty()) {
            return result;
        }
        try {
            Class clazz = Class.forName(className);
            formatter = (ResponseTransformerIface) clazz.getDeclaredConstructor().newInstance();
            // result = formatter.transform(originalResponse, devConfig,
            // service.getMessageService());
            result = formatter.transform(originalResponse, devConfig, null);
            if (LOG.isDebugEnabled()) {
                LOG.debug("response to transform:" + originalResponse + " size:" + originalResponse.length());
                LOG.debug("response transformed:" + result);
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            LOG.error(e.getMessage());
            throw new Exception("Result transformation error: " + e.getMessage());
        }
        return result;
    }

    private Map<String, String> getDedicatedResponderHeaders(Device device, String response) {
        if (null == device) {
            return null;
        }
        HashMap<String, Object> devConfig = device.getConfigurationMap();
        ResponseTransformerIface formatter;
        Map result = new HashMap<>();
        String className = (String) devConfig.get("formatter");
        if (null == className || className.isEmpty()) {
            return result;
        }
        try {
            Class clazz = Class.forName(className);
            formatter = (ResponseTransformerIface) clazz.getDeclaredConstructor().newInstance();
            result = formatter.getHeaders(devConfig, device.getConfiguration(), response);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            LOG.error(e.getMessage());
        }
        return result;
    }

    private IotData2 parseTextData(Device device, boolean authRequired, String input, String authKey,
            String separator) {
        IotData2 data = runDedicatedParser(device, input);
        if (null != data) {
            return data;
        }
        long systemTimestamp = System.currentTimeMillis();
        data = new IotData2(systemTimestamp);
        data.dev_eui = device.getEUI();
        HashMap<String, Object> options = new HashMap<>();
        options.put("separator", separator);
        // options.put("eui", eui);
        // options.put("euiInHeader", ""+euiHeaderFirst);
        PayloadParserIface parser = new com.signomix.receiver.PayloadParser();
        data.payload_fields = (ArrayList) parser.parse(input, options);
        if (!euiHeaderFirst || (null == data.dev_eui || data.dev_eui.isEmpty())) {
            data.dev_eui = getEuiParamValue(data.payload_fields);
        }
        data.normalize();
        //data.setTimestampUTC(systemTimestamp);
        data.authRequired = authRequired;
        data.authKey = authKey;
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

    private IotData2 parseFormData(String eui, boolean authRequired, MultivaluedMap<String, String> form,
            String authKey) {
        long systemTimestamp = System.currentTimeMillis();
        IotData2 data = new IotData2(systemTimestamp);
        data.dev_eui = eui;
        data.payload_fields = new ArrayList<>();
        HashMap<String, String> map;
        Iterator<String> it = form.keySet().iterator();
        String key;
        String value;
        while (it.hasNext()) {
            key = it.next();
            value = form.getFirst(key);
            if (LOG.isDebugEnabled()) {
                LOG.debug(key + "=" + value);
            }
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
        //data.setTimestampUTC(systemTimestamp);
        data.authRequired = authRequired;
        data.authKey = authKey;
        return data;
    }

    private IotData2 parseJson(String eui, boolean authRequired, String authKey, IotDto dataObject) {
        long systemTimestamp = System.currentTimeMillis();
        IotData2 data = new IotData2(systemTimestamp);
        data.dev_eui = eui;
        if (null != dataObject.dev_eui && !dataObject.dev_eui.isEmpty()) {
            data.dev_eui = dataObject.dev_eui;
        }
        data.gateway_eui = dataObject.gateway_eui;
        data.timestamp = "" + dataObject.timestamp;
        data.clientname = dataObject.clientname;
        data.payload = dataObject.payload;
        data.hexPayload = dataObject.hex_payload;
        data.payload_fields = dataObject.payload_fields;
        data.normalize();
        //data.setTimestampUTC(systemTimestamp);
        data.authKey = authKey;
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

    private Response isDeviceActive(String eui, String inHeaderEui) {
        // When eui in request header
        // Then device can be checked
        if (euiHeaderFirst) {
            Device device = service.getDevice(inHeaderEui);
            if (null == device) {
                LOG.warn("unknown device " + inHeaderEui);
                return Response.status(Status.BAD_REQUEST).entity("device not registered").build();
            }
            if (!device.isActive()) {
                return Response.status(Status.NOT_FOUND).entity("device is not active").build();
            }
        }
        return Response.ok().build();
    }

}