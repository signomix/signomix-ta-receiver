package com.signomix.receiver;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.jboss.logging.Logger;

import com.signomix.common.HexTool;
import com.signomix.common.db.IotDatabaseDao;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.event.IotEvent;
import com.signomix.common.event.MessageServiceIface;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.DeviceType;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.common.iot.virtual.VirtualData;
import com.signomix.receiver.script.NashornScriptingAdapter;
import com.signomix.receiver.script.ProcessorResult;
import com.signomix.receiver.script.ScriptAdapterException;
import io.agroal.api.AgroalDataSource;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;

@ApplicationScoped
public class ReceiverService {
    private static final Logger LOG = Logger.getLogger(ReceiverService.class);

    // TODO: test /q/health/ready

    @Inject
    AgroalDataSource ds;

    @Inject
    MessageServiceIface messageService;

    @Inject
    DataProcessor processor;

    @Inject
    NashornScriptingAdapter scriptingAdapter;
    // ScriptingAdapterIface scriptingAdapter;

    IotDatabaseIface dao;

    @ConfigProperty(name = "signomix.app.key", defaultValue = "not_configured")
    String appKey;
    @ConfigProperty(name = "signomix.core.host", defaultValue = "not_configured")
    String coreHost;
    @ConfigProperty(name = "device.status.update.integrated")
    Boolean deviceStatusUpdateIntegrated;

    public void onApplicationStart(@Observes StartupEvent event) {
        dao = new IotDatabaseDao();
        dao.setDatasource(ds);
    }

    public String processDataAndReturnResponse(IotData2 data) {
        return processData(data);
    }

    @ConsumeEvent(value = "iotdata-no-response")
    public void processDataNoResponse(IotData2 data) {
        processData(data);
    }

    @ConsumeEvent(value = "ttndata-no-response")
    void processTtnData(IotData2 data) {
        processData(data);
    }

    @ConsumeEvent(value = "chirpstackdata-no-response")
    void processChirpstackData(IotData2 data) {
        processData(data);
    }

    public MessageServiceIface getMessageService() {
        return messageService;
    }

    /**
     * Sends data to dedicated microservice
     * 
     * @param inputList
     * @param device
     * @param iotData
     * @param dataString
     * @return data processing result
     */
    private ProcessorResult callProcessorService(ArrayList<ChannelData> inputList, Device device, IotData2 iotData,
            String dataString) {
        // TODO
        return null;
    }

    private String processData(IotData2 data) {
        LOG.info("DATA FROM EUI: " + data.getDeviceEUI());
        String result = "";
        DeviceType[] expected = { DeviceType.GENERIC, DeviceType.VIRTUAL, DeviceType.TTN, DeviceType.CHIRPSTACK,
                DeviceType.LORA };
        String deviceId = data.deviceId;
        Device device = getDeviceChecked(data, expected);
        if (null == device) {
            // TODO: result.setData(authMessage);
            return result;
        }
        String parserError = getFirstParserErrorValue(data);
        if (null != parserError && !parserError.isEmpty()) {
            return "ERROR: " + parserError;
        }
        data.setTimestampUTC();
        data.prepareIotValues();
        ArrayList<ChannelData> inputList = decodePayload(data, device);
        for (int i = 0; i < inputList.size(); i++) {
            LOG.debug(inputList.get(i).toString());
        }
        ProcessorResult scriptResult = null;
        ArrayList<ArrayList> outputList;
        // String dataString = data.getSerializedData();
        String dataString = null;
        boolean statusUpdated = false;
        try {
            scriptResult = callProcessorService(inputList, device, data, dataString);
            if (null == scriptResult) {
                scriptResult = getProcessingResult(inputList, device, data, dataString);
            }
            // data to save
            LOG.info("outputList.size()==" + scriptResult.getOutput().size());
            outputList = scriptResult.getOutput();
            for (int i = 0; i < outputList.size(); i++) {
                saveData(device, outputList.get(i));
            }
            if (DeviceType.VIRTUAL.name().equals(device.getType())) {
                saveVirtualData(device, data);
            }
            // device status
            if (device.getState().compareTo(scriptResult.getDeviceState()) != 0) {
                LOG.info("updateDeviceStatus");
                updateDeviceStatus(device.getEUI(), device.getTransmissionInterval(), scriptResult.getDeviceState(),
                        device.ALERT_OK);
            } else if (device.isActive()) {
                Log.info("updateHealthStatus");
                updateHealthStatus(device.getEUI(), device.getTransmissionInterval(), device.getState(),
                        device.ALERT_OK);
            } else {
                LOG.debug("device: active " + device.isActive() + " status " + device.getState()
                        + " script device status " + scriptResult.getDeviceState());
            }
            statusUpdated = true;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage());
            addNotifications(device, null, e.getMessage(), false);
        }
        if (!statusUpdated) {
            updateHealthStatus(device.getEUI(), device.getTransmissionInterval(), device.getState(), device.ALERT_OK);
        }
        if (null == scriptResult) {
            return "";
        }

        ArrayList<IotEvent> events = scriptResult.getEvents();
        HashMap<String, String> recipients;

        // commands and notifications
        for (int i = 0; i < events.size(); i++) {
            LOG.debug("event " + i + " (" + device.getEUI() + ")");
            if (IotEvent.ACTUATOR_CMD.equals(events.get(i).getType())
                    || IotEvent.ACTUATOR_HEXCMD.equals(events.get(i).getType())
                    || IotEvent.ACTUATOR_PLAINCMD.equals(events.get(i).getType())) {
                // commands
                saveCommand(events.get(i));
            } else {
                // TODO: addNotifications
                addNotifications(device, (IotEvent) events.get(i).clone(), null, true);
                /*
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
                    try {
                        dao.addAlert(newEvent);
                    } catch (IotDatabaseException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    messageService.sendNotification(newEvent);
                }
                */
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

        // are commands waiting?
        try {
            IotEvent command = (IotEvent) dao.getFirstCommand(device.getEUI());
            if (null != command) {
                String commandPayload = (String) command.getPayload();
                if (IotEvent.ACTUATOR_HEXCMD.equals(command.getType())) {
                    String rawCmd = new String(
                            Base64.getEncoder().encode(HexTool.hexStringToByteArray(commandPayload)));
                    result = rawCmd;
                } else {
                    result = commandPayload;
                }
                LOG.debug("COMMANDID/PAYLOAD (" + device.getEUI() + "):" + command.getId() + "/" + commandPayload);
                dao.removeCommand(command.getId());
                dao.putCommandLog(command.getOrigin(), command);
            } else {
                LOG.debug("COMMANDID/PAYLOAD (" + device.getEUI() + ") IS NULL");
            }
        } catch (IotDatabaseException e) {
            e.printStackTrace();
        }
        return result;
    }

    private ProcessorResult getProcessingResult(ArrayList<ChannelData> inputList, Device device, IotData2 iotData,
            String dataString)
            throws Exception {
        ProcessorResult result = processor.getProcessingResult(inputList, device,
                iotData.getReceivedPackageTimestamp(), iotData.getLatitude(),
                iotData.getLongitude(), iotData.getAltitude(), dataString, "", dao);
        result.setApplicationConfig(device.getApplicationConfig());
        return result;
    }

    ArrayList<ChannelData> fixValues(Device device, ArrayList<ChannelData> values) {
        ArrayList<ChannelData> fixedList = new ArrayList<>();
        if (values != null && values.size() > 0) {
            for (ChannelData value : values) {
                if (device.getChannels().containsKey(value.getName())) {
                    fixedList.add(value);
                }
            }
        }
        return fixedList;
    }

    private void saveCommand(IotEvent commandEvent) {
        try {
            String[] origin = commandEvent.getOrigin().split("@");
            LOG.debug("saving command (" + origin[1] + ")");
            IotEvent ev = commandEvent;
            ev.setId(getNewCommandId(origin[1]));
            dao.putDeviceCommand(origin[1], commandEvent);
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void saveData(Device device, ArrayList<ChannelData> list) {
        try {
            LOG.info("saveData list.size():" + list.size());
            dao.putData(device, fixValues(device, list));
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void saveVirtualData(Device device, IotData2 data) {
        // TODO
        try {
            VirtualData vd = new VirtualData(data.getDeviceEUI());
            vd.timestamp = data.getTimestampUTC().getTime();
            Map tmp;
            String name;
            Double value;
            for (int i = 0; i < data.payload_fields.size(); i++) {
                tmp = data.payload_fields.get(i);
                name = (String) tmp.get("name");
                value = null;
                try {
                    value = (Double) tmp.get("value");
                } catch (Exception e) {
                    try {
                        value = ((Long) tmp.get("value")).doubleValue();
                    } catch (Exception e2) {
                        try {
                            value = Double.parseDouble((String) tmp.get("value"));
                        } catch (Exception e1) {
                            LOG.warn("unable to parse " + name + " value: " + tmp.get("value"));
                        }
                    }
                }
                if (null != value) {
                    vd.payload_fields.put(name, value);
                }
            }
            dao.putVirtualData(device, vd);
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void updateDeviceStatus(String eui, long transmissionInterval, Double newStatus, int newAlertStatus) {
        if (!deviceStatusUpdateIntegrated) {
            // TEST
            LOG.info("Device status update skipped.");
            return;
        }
        try {
            dao.updateDeviceStatus(eui, transmissionInterval, newStatus, newAlertStatus);
            LOG.info("Device status updated.");
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }

    private void updateHealthStatus(String eui, long transmissionInterval, Double newStatus, int newAlertStatus) {
        if (!deviceStatusUpdateIntegrated) {
            // TEST
            LOG.info("Device health status update skipped.");
            return;
        }
        try {
            dao.updateDeviceStatus(eui, transmissionInterval, newStatus, newAlertStatus);
            LOG.info("Device health status updated.");
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }

    private ArrayList<ChannelData> decodePayload(IotData2 data, Device device) {
        if (null == device) {
            LOG.warn("device is null");
            return new ArrayList<>();
        }
        if (!data.getDataList().isEmpty()) {
            LOG.debug("data list not empty");
            return data.getDataList();
        }
        ArrayList<ChannelData> values = new ArrayList<>();
        if (data.getPayloadFieldNames() == null || data.getPayloadFieldNames().length == 0) {
            if (null != data.getPayload()) {
                LOG.debug("base64Payload: " + data.getPayload());
                Decoder decoder = Base64.getDecoder();
                if (null == decoder) {
                    LOG.warn("decoder is null");
                    return values;
                }
                byte[] decodedPayload = decoder.decode(data.getPayload().getBytes());
                if (null == decodedPayload) {
                    LOG.warn("decodedPayload is null");
                    return values;
                }
                try {
                    values = scriptingAdapter.decodeData(decodedPayload, device, data.getTimestamp());
                } catch (ScriptAdapterException ex) {
                    ex.printStackTrace();
                    addNotifications(device, null, ex.getMessage(), false);
                    return values;
                } catch (Exception e) {
                    e.printStackTrace();
                    addNotifications(device, null, e.getMessage(), false);
                    return values;
                }
            } else if (null != data.getHexPayload()) {
                LOG.debug("hexPayload: " + data.getHexPayload());
                byte[] byteArray = getByteArray(data.getHexPayload());
                if (null == byteArray) {
                    LOG.warn("decodedPayload is null");
                    return values;
                }
                try {
                    values = scriptingAdapter.decodeData(byteArray, device, data.getTimestamp());
                } catch (ScriptAdapterException ex) {
                    ex.printStackTrace();
                    addNotifications(device, null, ex.getMessage(), false);
                    return values;
                } catch (Exception e) {
                    e.printStackTrace();
                    addNotifications(device, null, e.getMessage(), false);
                    return values;
                }
            } else {
                LOG.warn("payload_fields nor payload send");
            }
        } else {
            for (int i = 0; i < data.payload_fields.size(); i++) {
                HashMap map = (HashMap) data.payload_fields.get(i);
            }
        }
        return values;
    }

    private void addNotifications(Device device, IotEvent event, String errorMessage, boolean withMessage) {
        HashMap<String, String> recipients = new HashMap<>();
        recipients.put(device.getUserID(), "");
        if (device.getTeam() != null) {
            String[] r = device.getTeam().split(",");
            for (int j = 0; j < r.length; j++) {
                if (!r[j].isEmpty()) {
                    recipients.put(r[j], "");
                }
            }
        }
        IotEvent errEvent=null;
        if(null!=errorMessage) {
            errEvent = new IotEvent("info",errorMessage);
        }
        Iterator itr = recipients.keySet().iterator();
        while (itr.hasNext()) {
            if (null != event) {
                event.setOrigin(itr.next() + "\t" + device.getEUI());
                try {
                    dao.addAlert(event);
                } catch (IotDatabaseException e) {
                    e.printStackTrace();
                }
                if (withMessage) {
                    messageService.sendNotification(event);
                }
            }
            if(null!=errEvent) {
                try {
                    errEvent.setOrigin(itr.next() + "\t" + device.getEUI());
                    dao.addAlert(errEvent);
                } catch (IotDatabaseException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private byte[] getByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
        /*
         * // converting string to integer value
         * int val = Integer.parseInt(input, 16);
         * // converting integer value to Byte Array
         * BigInteger big = BigInteger.valueOf(val);
         * return big.toByteArray();
         */
    }

    public Device getDevice(String eui) {
        LOG.debug("getDevice");
        Device device = null;
        // Device gateway = null;
        try {
            device = dao.getDevice(eui, false);
            // gateway = getDevice(data.getGatewayEUI());
        } catch (IotDatabaseException e) {
            LOG.error(e.getMessage());
        }
        return device;
    }

    long getNewCommandId(String deviceEUI) throws Exception {
        try {
            try {
                CoreSystemService client = RestClientBuilder.newBuilder()
                        .baseUri(new URI(coreHost + "/api/system/commandid"))
                        .followRedirects(true)
                        .build(CoreSystemService.class);
                long result;
                try {
                    result = (Long) client.getNewCommandId(appKey, deviceEUI).get("value");
                } catch (Exception e) {
                    result = (Integer) client.getNewCommandId(appKey, deviceEUI).get("value");
                }
                return result;
            } catch (URISyntaxException ex) {
                LOG.error(ex.getMessage());
                // TODO: notyfikacja użytkownika o błędzie
                throw new Exception();
            } catch (ProcessingException ex) {
                LOG.error(ex.getMessage());
                throw new Exception();
            } catch (WebApplicationException ex) {
                ex.printStackTrace();
                LOG.error(ex.getMessage());
                System.out.println("WEB APP EXCEPTION");
                throw new Exception();
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
                // TODO: notyfikacja użytkownika o błędzie
                throw new Exception();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new Exception();
        }
    }

    public Device getDeviceChecked(String eui, String authKey, boolean authRequired, DeviceType[] expectedTypes) {
        Device gateway = null;
        Device device = getDevice(eui);
        if (null == device) {
            LOG.warn("Device " + eui + " is not registered");
            return null;
        }
        if (authRequired) {
            String secret;
            if (gateway == null) {
                secret = device.getKey();
            } else {
                secret = gateway.getKey();
            }
            try {
                if (null == authKey || !authKey.equals(secret)) {
                    LOG.warn("Authorization key don't match for " + device.getEUI() + " :" + authKey + ":" + secret);
                    return null;
                }
            } catch (Exception ex) { // catch (UserException ex) {
                ex.printStackTrace();
                LOG.warn(ex.getMessage());
                return null;
            }
        }

        boolean deviceFound = false;
        for (int i = 0; i < expectedTypes.length; i++) {
            if (expectedTypes[i] == DeviceType.valueOf(device.getType())) {
                deviceFound = true;
                break;
            }
        }
        if (!deviceFound) {
            LOG.warn("Device " + eui + " type is not valid");
            return null;
        }
        if (!device.isActive()) {
            // TODO: return "device is not active"?;
            return null;
        }
        return device;
    }

    private Device getDeviceChecked(IotData2 data, DeviceType[] expectedTypes) {
        return getDeviceChecked(data.getDeviceEUI(), data.getAuthKey(), data.authRequired, expectedTypes);
    }

    private String getFirstParserErrorValue(IotData2 data) {
        Map map;
        for (int i = 0; i < data.payload_fields.size(); i++) {
            map = data.payload_fields.get(i);
            if (null != map.get("parser_error")) {
                return (String) map.get("parser_error");
            }
        }
        return "";
    }

}
