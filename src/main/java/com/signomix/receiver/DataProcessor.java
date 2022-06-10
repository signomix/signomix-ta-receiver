/**
 * Copyright (C) Grzegorz Skorupa 2018.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.receiver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.receiver.event.IotEvent;
import com.signomix.receiver.script.NashornScriptingAdapter;
import com.signomix.receiver.script.ScriptAdapterException;
import com.signomix.receiver.script.ProcessorResult;

import org.jboss.logging.Logger;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
@ApplicationScoped
public class DataProcessor {
    private static final Logger LOG = Logger.getLogger(DataProcessor.class);

    @Inject
    MessageService messageService;

    @Inject
    NashornScriptingAdapter scriptingAdapter;

    public Object[] processValues(
            ArrayList<ChannelData> listOfValues,
            Device device,
            long dataTimestamp,
            Double latitude,
            Double longitude,
            Double altitude,
            String requestData,
            String command,
            IotDatabaseIface dao) throws Exception {
        //ScriptingAdapterIface scriptingAdapter = new GraalVMScriptingAdapter();
        ProcessorResult scriptResult = null;
        try {
            LOG.debug("listOfValues.size()=="+listOfValues.size());
            scriptResult = scriptingAdapter.processData1(
                    listOfValues,
                    device,
                    dataTimestamp,
                    latitude,
                    longitude,
                    altitude,
                    command,
                    requestData,
                    dao);
        } catch (ScriptAdapterException e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        if (scriptResult == null) {
            throw new Exception("preprocessor script returns null result");
        }

        ArrayList<IotEvent> events = scriptResult.getEvents();
        HashMap<String, String> recipients;
        // commands and notifications
        for (int i = 0; i < events.size(); i++) {
            if (IotEvent.ACTUATOR_CMD.equals(events.get(i).getType())
                    || IotEvent.ACTUATOR_HEXCMD.equals(events.get(i).getType())) {
                        messageService.sendCommand(events.get(i));
            } else {
                recipients = new HashMap<>();
                recipients.put(device.getUserID(), "");
                if (device.getTeam() != null) {
                    String[] r = device.getTeam().split(",");
                    for (int j = 0; j < r.length; j++) {
                        if (!r[j].isEmpty()) {
                            recipients.put(r[j], "");
                        }
                    }
                }
                Iterator itr = recipients.keySet().iterator();
                while (itr.hasNext()) {
                    IotEvent newEvent = (IotEvent) events.get(i).clone();
                    newEvent.setOrigin(itr.next() + "\t" + device.getEUI());
                    messageService.sendNotification(newEvent);
                }
            }
        }
        // data events
        HashMap<String, ArrayList> dataEvents = scriptResult.getDataEvents();
        ArrayList<IotEvent> el;
        for (String key : dataEvents.keySet()) {
            el = dataEvents.get(key);
            IotEvent newEvent;
            if (el.size() > 0) {
                newEvent = (IotEvent) el.get(0).clone();
                // newEvent.setOrigin(device.getUserID());
                String payload = "";
                for (int i = 0; i < el.size(); i++) {
                    payload = payload + ";" + el.get(i).getPayload();
                }
                payload = payload.substring(1);
                newEvent.setPayload(payload);
                messageService.sendData(newEvent);
            }
        }
        Object[] result = { 
            scriptResult.getDeviceState(),
            scriptResult.getOutput()
        };
        return result;
    }

    public ProcessorResult getProcessingResult(
            ArrayList<ChannelData> listOfValues,
            Device device,
            long dataTimestamp,
            Double latitude,
            Double longitude,
            Double altitude,
            String requestData,
            String command,
            IotDatabaseIface dao) throws Exception {
        ProcessorResult scriptResult = null;
        try {
            LOG.debug("listOfValues.size()==" + listOfValues.size());
            LOG.info("listOfValues.size()==" + listOfValues.size());
            scriptResult = scriptingAdapter.processData1(
                    listOfValues,
                    device,
                    dataTimestamp,
                    latitude,
                    longitude,
                    altitude,
                    command,
                    requestData,
                    dao);
        } catch (ScriptAdapterException e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        if (scriptResult == null) {
            throw new Exception("preprocessor script returns null result");
        }
        return scriptResult;
    }

}
