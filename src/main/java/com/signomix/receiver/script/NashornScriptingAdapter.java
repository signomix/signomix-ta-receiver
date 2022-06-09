/**
 * Copyright (C) Grzegorz Skorupa 2018,2022.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.receiver.script;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.receiver.IotDatabaseIface;
import com.signomix.receiver.MessageService;
import com.signomix.receiver.event.IotEvent;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import io.quarkus.runtime.StartupEvent;

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
    MessageService messageService;

    private ScriptEngine engine;
    private String processorScript;
    private String decoderScript;

    public void onApplicationStart(@Observes StartupEvent event) {
        processorScript = readScript(processorScriptLocation);
        decoderScript = readScript(decoderScriptLocation);
        LOG.debug("processor: "+processorScript);
        LOG.debug("decoder: "+decoderScript);
        engine=new ScriptEngineManager().getEngineByName("nashorn");
        LOG.debug("engine: "+engine);
    }

    @Override
    public ScriptResult processData1(ArrayList<ChannelData> values,
            Device device,
            long dataTimestamp,
            Double latitude, Double longitude, Double altitude,
            String command, String requestData, IotDatabaseIface dao) throws ScriptAdapterException {

        String deviceScript = device.getCodeUnescaped();
        String deviceID = device.getEUI();
        String userID = device.getUserID();

        Double state = device.getState();
        int alert = device.getAlertStatus();
        Double devLatitude = device.getLatitude();
        Double devLongitude = device.getLongitude();
        Double devAltitude = device.getAltitude();

        String deviceConfig=device.getConfiguration();
        HashMap<String,Object> applicationConfig=device.getApplicationConfig();

        Invocable invocable;
        ScriptResult result = new ScriptResult();
        if (values == null) {
            return result;
        }
        LOG.debug("values.size=="+values.size());
        for(int i=0; i<values.size(); i++){
            LOG.debug(values.get(i).toString());
        }
        ChannelClient channelReader = new ChannelClient(userID, deviceID, dao);
        try {
            engine.eval(deviceScript != null ? merge(processorScript, deviceScript) : processorScript);
            invocable = (Invocable) engine;
            result = (ScriptResult) invocable.invokeFunction("processData", deviceID, values, channelReader, userID,
                    dataTimestamp, latitude, longitude, altitude, state, alert,
                    devLatitude, devLongitude, devAltitude, command, requestData,deviceConfig,applicationConfig);
                    LOG.debug("result.output.size=="+result.getOutput().size());
                    LOG.debug("result.measures.size=="+result.getMeasures().size());
        } catch (NoSuchMethodException e) {
            fireEvent(2, device, e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.NO_SUCH_METHOD,
                    "ScriptingAdapter.no_such_method " + e.getMessage());
        } catch (ScriptException e) {
            fireEvent(2, device, e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.SCRIPT_EXCEPTION,
                    "ScriptingAdapter.script_exception " + e.getMessage());
        }
        return result;
    }

    @Override
    public ArrayList<ChannelData> decodeData(byte[] data, Device device, long timestamp)
            throws ScriptAdapterException {
        Invocable invocable;
        ArrayList<ChannelData> list = new ArrayList<>();
        try {
            engine.eval(device.getEncoderUnescaped() != null ? merge(decoderScript, device.getEncoderUnescaped()) : decoderScript);
            invocable = (Invocable) engine;
            list = (ArrayList) invocable.invokeFunction("decodeData", device.getDeviceID(), data, timestamp);
        } catch (NoSuchMethodException e) {
            fireEvent(1, device, e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.NO_SUCH_METHOD, e.getMessage());
        } catch (ScriptException e) {
            fireEvent(1, device, e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.SCRIPT_EXCEPTION, e.getMessage());
        }
        return list;
    }

    @Override
    public ArrayList<ChannelData> decodeHexData(String hexadecimalPayload, Device device,
            long timestamp) throws ScriptAdapterException {
        Invocable invocable;
        ArrayList<ChannelData> list = new ArrayList<>();
        try {
            engine.eval(device.getEncoderUnescaped() != null ? merge(decoderScript, device.getEncoderUnescaped()) : decoderScript);
            invocable = (Invocable) engine;
            list = (ArrayList) invocable.invokeFunction("decodeHexData", device.getDeviceID(), hexadecimalPayload, timestamp);
        } catch (NoSuchMethodException e) {
            fireEvent(1, device, e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.NO_SUCH_METHOD, e.getMessage());
        } catch (ScriptException e) {
            fireEvent(1, device, e.getMessage());
            throw new ScriptAdapterException(ScriptAdapterException.SCRIPT_EXCEPTION, e.getMessage());
        }
        return list;
    }

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
     * Sends information about eroros
     * 
     * @param source
     * @param origin
     * @param message
     */
    private void fireEvent(int source, Device device, String message) {
        IotEvent ev = new IotEvent();
        ev.setOrigin(device.getUserID() + "\t" + device.getDeviceID());
        if (source == 1) {
            ev.setPayload("Decoder script (1): " + message);
        } else {
            ev.setPayload("Data processor script (1): " + message);
        }
        ev.setType(IotEvent.GENERAL);
        messageService.sendErrorInfo(ev);

    }

}
