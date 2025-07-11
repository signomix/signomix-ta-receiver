/**
 * Copyright (C) Grzegorz Skorupa 2018,2022.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.receiver.script;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.EventEnvelope;
import com.signomix.common.Tag;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.Application;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.receiver.processor.ProcessorResult;

import io.quarkus.runtime.StartupEvent;
import io.questdb.std.Hash;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

/**
 *
 * @author greg
 */
@ApplicationScoped
public class NashornScriptingAdapter implements ScriptingAdapterIface {
    private static final Logger LOG = Logger.getLogger(NashornScriptingAdapter.class);

    @ConfigProperty(name = "decoder.script")
    private String decoderScriptLocation;

    @ConfigProperty(name = "processor.script")
    String processorScriptLocation;

    @Inject
    @Channel("error-event")
    Emitter<String> errorEventEmitter;

    // @Inject
    // MessageServiceIface messageService;

    private ScriptEngine engine;
    private String processorScript;
    private String decoderScript;

    public void onApplicationStart(@Observes StartupEvent event) {
        processorScript = readScript(processorScriptLocation);
        decoderScript = readScript(decoderScriptLocation);
        LOG.debug("processor: " + processorScript);
        LOG.debug("decoder: " + decoderScript);
        try {
            new ScriptEngineManager().getEngineFactories().forEach(f -> {
                LOG.info("engine factory: " + f.getEngineName());
            });
            engine = new ScriptEngineManager().getEngineByName("nashorn");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        LOG.info("engine: " + engine);
    }

    private ScriptEngine getEngine() {
        if (engine == null) {
            LOG.info("init engine by mime type");
            engine = new ScriptEngineManager().getEngineByMimeType("text/javascript");
            LOG.info("engine: " + engine);
        }
        return engine;
    }

    @Override
    public ProcessorResult processData1(
            ArrayList<ChannelData> values,
            Device device,
            Application application,
            long dataTimestamp,
            Double latitude, Double longitude, Double altitude,
            String command, String requestData, IotDatabaseIface dao,
            Long port) throws ScriptAdapterException {

        String deviceScript = device.getCodeUnescaped();
        String deviceID = device.getEUI();
        String userID = device.getUserID();

        Double state = device.getState();
        int alert = device.getAlertStatus();
        Double devLatitude = device.getLatitude();
        Double devLongitude = device.getLongitude();
        Double devAltitude = device.getAltitude();

        // String deviceConfig = device.getConfiguration();
        HashMap<String, Object> deviceConfig = device.getConfigurationMap();
        HashMap<String, Object> applicationConfig = device.getApplicationConfig();
        List<Tag> tags = device.getTagsAsList();
        HashMap<String, Object> deviceTags = new HashMap<>();
        for (Tag tag : tags) {
            deviceTags.put(tag.name, tag.value);
        }

        String deviceGroups = device.getGroups();

        Set<String> availableZoneIds = ZoneId.getAvailableZoneIds();

        // Convert the set to an array
        String[] timeZones = availableZoneIds.toArray(new String[0]);
        HashMap<String, Integer> offsets = getTimeZoneOffsets(timeZones);

        Invocable invocable;
        ProcessorResult result = new ProcessorResult();
        if (values == null) {
            return result;
        }
        LOG.debug("values.size==" + values.size());
        for (int i = 0; i < values.size(); i++) {
            LOG.debug(values.get(i).toString());
        }
        ChannelClient channelReader = new ChannelClient(userID, deviceID, dao);
        GroupClient groupReader = new GroupClient(userID, deviceGroups, dao);

        /* if ((deviceScript == null || deviceScript.trim().isEmpty()) && application != null) {
            deviceScript = application.code;
        } */
        if (deviceScript == null) {
            deviceScript = "";
        }
        if(application != null && application.code != null){
            deviceScript += "\n" + application.code;  
        }
        ScriptEngine engine = getEngine();
        try {
            engine.eval(deviceScript != null ? merge(processorScript, deviceScript) : processorScript);
            invocable = (Invocable) engine;
            result = (ProcessorResult) invocable.invokeFunction("processData", deviceID, values,
                    channelReader, groupReader,
                    userID, dataTimestamp, state, alert,
                    devLatitude, devLongitude, devAltitude, command, requestData, deviceConfig, applicationConfig,
                    deviceGroups,
                    offsets, port, deviceTags);
            LOG.debug("result.output.size==" + result.getOutput().size());
            LOG.debug("result.measures.size==" + result.getMeasures().size());
        } catch (NoSuchMethodException e) {
            LOG.warn(e.getMessage());
            fireEvent(2, device.getEUI(), e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.NO_SUCH_METHOD,
                    "ScriptingAdapter.no_such_method " + e.getMessage());
        } catch (ScriptException e) {
            LOG.warn(e.getMessage());
            fireEvent(2, device.getEUI(), e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.SCRIPT_EXCEPTION,
                    "ScriptingAdapter.script_exception " + e.getMessage());
        }
        return result;
    }

    @Override
    public ArrayList<ChannelData> decodeData(byte[] data, String deviceEui, String deviceDecoderScript, long timestamp)
            throws ScriptAdapterException {
        Invocable invocable;
        ArrayList<ChannelData> list = new ArrayList<>();
        try {
            LOG.debug("DECODING PAYLOAD");
            if (decoderScript == null || decoderScript.trim().isEmpty()) {
                return list;
            }
            String mergedScript = deviceDecoderScript != null ? merge(decoderScript, deviceDecoderScript)
                    : decoderScript;
            LOG.debug(decoderScript);
            ScriptEngine engine = getEngine();
            engine.eval(mergedScript);
            invocable = (Invocable) engine;
            list = (ArrayList) invocable.invokeFunction("decodeData", deviceEui, data, timestamp);
        } catch (NoSuchMethodException e) {
            LOG.warn(e.getMessage());
            fireEvent(1, deviceEui, e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.NO_SUCH_METHOD, e.getMessage());
        } catch (ScriptException e) {
            LOG.warn(e.getMessage());
            fireEvent(1, deviceEui, e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.SCRIPT_EXCEPTION, e.getMessage());
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            fireEvent(1, deviceEui, e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.SCRIPT_EXCEPTION, e.getMessage());
        }
        return list;
    }

    /**
     * not used
     */
    /*
     * @Override
     * public ArrayList<ChannelData> decodeHexData(String hexadecimalPayload, Device
     * device,
     * long timestamp) throws ScriptAdapterException {
     * Invocable invocable;
     * ArrayList<ChannelData> list = new ArrayList<>();
     * try {
     * engine.eval(device.getEncoderUnescaped() != null ? merge(decoderScript,
     * device.getEncoderUnescaped()) : decoderScript);
     * invocable = (Invocable) engine;
     * list = (ArrayList) invocable.invokeFunction("decodeHexData",
     * device.getDeviceID(), hexadecimalPayload, timestamp);
     * } catch (NoSuchMethodException e) {
     * fireEvent(1, device, e.getMessage());
     * throw new ScriptAdapterException(ScriptAdapterException.NO_SUCH_METHOD,
     * e.getMessage());
     * } catch (ScriptException e) {
     * fireEvent(1, device, e.getMessage());
     * throw new ScriptAdapterException(ScriptAdapterException.SCRIPT_EXCEPTION,
     * e.getMessage());
     * }catch(Exception e){
     * fireEvent(1, device, e.getMessage());
     * throw new ScriptAdapterException(ScriptAdapterException.SCRIPT_EXCEPTION,
     * e.getMessage());
     * }
     * return list;
     * }
     */

    String merge(String template, String deviceScript) {
        String res = template.replaceAll("//injectedCode", deviceScript);
        return res;
    }

    /**
     * Reads script from file
     *
     * @param path the file location
     * @return script content
     */
    public String readScript(String path) {
        LOG.debug("reading " + path);
        String result;
        InputStream resource = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(resource, "UTF-8"))) {
            result = br.lines().collect(Collectors.joining(System.lineSeparator()));
        } catch (IOException ex) {
            LOG.error(ex.getMessage());
            return null;
        }
        return result;
    }

    /**
     * Sends information about errors
     * 
     * @param source
     * @param origin
     * @param message
     */
    private void fireEvent(int source, String deviceEui, String message) {
        /*
         * IotEvent ev = new IotEvent();
         * ev.setOrigin(device.getUserID() + "\t" + device.getDeviceID());
         * if (source == 1) {
         * ev.setPayload("Decoder script (1): " + message);
         * } else {
         * ev.setPayload("Data processor script (1): " + message);
         * }
         * ev.setType(IotEvent.GENERAL);
         * messageService.sendErrorInfo(ev);
         */
        String payload;
        if (source == 1) {
            payload = "Decoder script (1): " + message;
        } else {
            payload = "Data processor script (1): " + message;
        }
        EventEnvelope wrapper = new EventEnvelope();
        wrapper.type = EventEnvelope.ERROR;
        wrapper.eui = deviceEui;
        wrapper.payload = payload;
        // messageService.sendEvent(wrapper);
        String encodedMessage;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            encodedMessage = objectMapper.writeValueAsString(wrapper);
            errorEventEmitter.send(encodedMessage);
        } catch (JsonProcessingException ex) {
            LOG.error(ex.getMessage());
        }

    }

    private HashMap<String, Integer> getTimeZoneOffsets(String[] timeZones) {
        HashMap<String, Integer> timeZoneOffsets = new HashMap<>();

        for (String timeZone : timeZones) {
            ZoneId zoneId = ZoneId.of(timeZone);
            ZonedDateTime zdt = ZonedDateTime.now(zoneId);
            int offsetInMinutes = zdt.getOffset().getTotalSeconds() / 60;

            timeZoneOffsets.put(timeZone, offsetInMinutes);
        }

        return timeZoneOffsets;
    }

}
