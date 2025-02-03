package com.signomix.receiver;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import com.signomix.common.HexTool;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.event.IotEvent;
import com.signomix.common.iot.Application;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.DeviceType;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.common.iot.sentinel.Signal;
import com.signomix.common.iot.virtual.VirtualData;
import com.signomix.common.tsdb.ApplicationDao;
import com.signomix.common.tsdb.IotDatabaseDao;
import com.signomix.common.tsdb.SignalDao;
import com.signomix.receiver.script.NashornScriptingAdapter;
import com.signomix.receiver.script.ProcessorResult;
import com.signomix.receiver.script.ScriptAdapterException;

import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class ReceiverService {

    @Inject
    Logger LOG;

    // TODO: test /q/health/ready

    @Inject
    @DataSource("oltp")
    AgroalDataSource tsDs;

    @Inject
    @DataSource("olap")
    AgroalDataSource olapDs;

    @Inject
    DataProcessor processor;

    @Inject
    NashornScriptingAdapter scriptingAdapter;

    @Inject
    BulkDataLoader bulkDataLoader;

    @Inject
    @Channel("data-received")
    Emitter<String> emitter;
    @Inject
    @Channel("data-created")
    Emitter<String> dataCreatedEmitter;
    @Inject
    @Channel("alerts")
    Emitter<String> alertEmitter;

    IotDatabaseDao dao = new IotDatabaseDao();
    IotDatabaseDao olapDao = new IotDatabaseDao();
    SignalDao signalDao = new SignalDao();
    ApplicationDao appDao = new ApplicationDao();

    private static AtomicLong commandIdSeed = null;
    private static AtomicLong eventSeed = new AtomicLong(System.currentTimeMillis());

    @Inject
    EventBus bus;

    @ConfigProperty(name = "device.status.update.integrated")
    Boolean deviceStatusUpdateIntegrated;
    @ConfigProperty(name = "signomix.database.type")
    String databaseType;
    @ConfigProperty(name = "signomix.command_id.bytes", defaultValue = "0")
    Short commandIdBytes;
    @ConfigProperty(name = "signomix.devices.protected", defaultValue = "false")
    Boolean useProtectedFeature;

    public void onApplicationStart(@Observes StartupEvent event) {
        dao.setDatasource(tsDs);
        olapDao.setDatasource(tsDs);
        olapDao.setAnalyticDatasource(olapDs);
        signalDao.setDatasource(tsDs);
        appDao.setDatasource(tsDs);
    }

    public String processDataAndReturnResponse(IotData2 data) {
        return processData(data);
    }

    public BulkLoaderResult processCsv(Device device, MultipartFormDataInput input, boolean singleDevice) {
        //if (null != dao) {
            return bulkDataLoader.loadBulkData(device, olapDao, input, singleDevice);
        //}
        //return null;
    } 
    
    public BulkLoaderResult processCsvString(Device device, String input) {
        //if (null != dao) {
            return bulkDataLoader.loadBulkData(device, olapDao, input);
        //}
        //return null;
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

    @ConsumeEvent(value = "virtualdata-no-response")
    void processVirtualData(String payload) {
        LOG.debug("virtualdata-no-response: " + payload);
        parseBusMessage(payload);
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
    private ProcessorResult callProcessorService(ArrayList<ChannelData> inputList, Device device,
            Application application, IotData2 iotData, String dataString) {
        // TODO
        return null;
    }

    private void parseBusMessage(String payload) {
        LOG.debug("parseBusMessage: " + payload);
        String[] parts = payload.split(";");
        // sort parts array basing on the first field (deviceId) - fields are separated
        // by ":"
        Arrays.sort(parts, new Comparator<String>() {
            public int compare(String s1, String s2) {
                String eui1 = s1.split(":")[0];
                String eui2 = s2.split(":")[0];
                return eui1.compareTo(eui2);
            }
        });
        String tmpEui = "";
        String[] dataObj;
        HashMap<String, String> map;
        IotData2 iotData = new IotData2();
        iotData.payload_fields = new ArrayList<>();
        for (String part : parts) {
            LOG.debug("part: " + part);
            dataObj = part.split(":");
            if (!tmpEui.equals(dataObj[0])) {
                // save previous iotData
                if (iotData != null && iotData.dev_eui != null && iotData.dev_eui.length() > 0) {
                    LOG.info("PROCESSING DATA FROM EUI: " + iotData.dev_eui);
                    iotData.normalize();
                    iotData.setTimestampUTC();
                    processData(iotData);
                }
                tmpEui = dataObj[0];
                iotData = new IotData2();
                iotData.dev_eui = dataObj[0];
                if (dataObj.length > 3) {
                    iotData.timestamp = dataObj[3];
                } else {
                    iotData.timestamp = "" + System.currentTimeMillis();
                }
                iotData.payload_fields = new ArrayList<>();
            }
            map = new HashMap<>();
            map.put("name", dataObj[1]);
            map.put("value", dataObj[2]);
            iotData.payload_fields.add(map);
        }
        if (iotData != null && iotData.dev_eui != null && iotData.dev_eui.length() > 0) {
            LOG.info("PROCESSING DATA FROM EUI: " + iotData.dev_eui);
            iotData.normalize();
            iotData.setTimestampUTC();
            processData(iotData);
        }
    }

    private String processData(IotData2 data) {
        LOG.info("DATA FROM EUI: " + data.getDeviceEUI());
        String result = "";
        DeviceType[] expected = { DeviceType.GENERIC, DeviceType.VIRTUAL, DeviceType.TTN, DeviceType.CHIRPSTACK,
                DeviceType.LORA };
        Device device = getDeviceChecked(data, expected);
        if (null == device) {
            // TODO: result.setData(authMessage);
            return null;
        }
        String parserError = getFirstParserErrorValue(data);
        if (null != parserError && !parserError.isEmpty()) {
            return "ERROR: " + parserError;
        }
        data.setTimestampUTC();
        data.prepareIotValues();
        Application app = getApplication(device.getOrgApplicationId());
        if (null == app) {
            LOG.info("app is null");
        } else {
            LOG.info("app code: " + app.code);
        }
        ArrayList<ChannelData> inputList = decodePayload(data, device, app);
        for (int i = 0; i < inputList.size(); i++) {
            LOG.debug(inputList.get(i).toString());
        }
        ProcessorResult scriptResult = null;
        ArrayList<ArrayList> outputList;
        String dataString = null;
        boolean statusUpdated = false;
        try {
            scriptResult = callProcessorService(inputList, device, app, data, dataString);
            if (null == scriptResult) {
                scriptResult = getProcessingResult(inputList, device, app, data, dataString);
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
            // addNotifications(device, null, e.getMessage(), false);
        }
        if (!statusUpdated) {
            updateHealthStatus(device.getEUI(), device.getTransmissionInterval(), device.getState(), device.ALERT_OK);
        }
        if (null == scriptResult) {
            return "";
        }

        ArrayList<IotEvent> events = scriptResult.getEvents();

        // commands and notifications
        for (int i = 0; i < events.size(); i++) {
            LOG.debug("event " + i + " (" + device.getEUI() + ")");
            if (IotEvent.ACTUATOR_CMD.equals(events.get(i).getType())
                    || IotEvent.ACTUATOR_HEXCMD.equals(events.get(i).getType())
                    || IotEvent.ACTUATOR_PLAINCMD.equals(events.get(i).getType())) {
                // commands
                saveCommand(events.get(i));
            } else {
                // notifications
                addNotifications(device, (IotEvent) events.get(i).clone(), null, true);
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
                String payload = "";
                for (int i = 0; i < el.size(); i++) {
                    if (i > 0) {
                        payload = payload + ";";
                    }
                    payload = payload + el.get(i).getPayload();
                }
                newEvent.setPayload(payload);
                LOG.info("SENDING DATA CREATED EVENT (" + device.getEUI() + "): " + newEvent.getPayload());
                // send event to mqtt
                dataCreatedEmitter.send((String) newEvent.getPayload());
                // send event to event bus
                sentToEventBus(payload);

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

    private Application getApplication(Long appId) {
        Application app = null;
        try {
            app = appDao.getApplication(appId);
        } catch (IotDatabaseException e) {
            LOG.warn(e.getMessage());
        }
        return app;
    }

    private void sentToEventBus(String payload) {
        // IotDataMessageCodec iotDataCodec = new IotDataMessageCodec();
        // DeliveryOptions options = new
        // DeliveryOptions().setCodecName(iotDataCodec.name());
        LOG.debug("sending to event bus: " + payload);
        bus.send("virtualdata-no-response", payload);
    }

    private ProcessorResult getProcessingResult(ArrayList<ChannelData> inputList, Device device,
            Application application, IotData2 iotData,
            String dataString)
            throws Exception {
        ProcessorResult result = processor.getProcessingResult(inputList, device, application,
                iotData.getReceivedPackageTimestamp(), iotData.getLatitude(),
                iotData.getLongitude(), iotData.getAltitude(), dataString, "", olapDao);
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
            if (null != dao) {
                dao.putData(device, fixValues(device, list));
            }
            if (null != olapDao) {
                LOG.debug("saveData to olap DB");
                olapDao.saveAnalyticData(device, fixValues(device, list));
            } else {
                LOG.warn("olapDao is null");
            }

            emitter.send(device.getEUI());
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
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
            if (null != dao) {
                dao.putVirtualData(device, vd);
            }
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
            if (null != dao) {
                dao.updateDeviceStatus(eui, transmissionInterval, newStatus, newAlertStatus);
            }
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
            if (null != dao) {
                dao.updateDeviceStatus(eui, transmissionInterval, newStatus, newAlertStatus);
            }
            LOG.info("Device health status updated.");
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }

    private ArrayList<ChannelData> decodePayload(IotData2 data, Device device, Application application) {
        if (null == device) {
            LOG.warn("device is null");
            return new ArrayList<>();
        }
        ArrayList<ChannelData> values = new ArrayList<>();
        byte[] emptyBytes = {};
        byte[] byteArray = null;
        String decoderScript = device.getEncoderUnescaped();
        if ((null == decoderScript || decoderScript.trim().isEmpty()) && null != application) {
            decoderScript = application.decoder;
        }
        if (null != decoderScript && decoderScript.length() > 0) {
            if (null != data.getPayload()) {
                LOG.debug("base64Payload: " + data.getPayload());
                Decoder base64Decoder = Base64.getDecoder();
                if (null == base64Decoder) {
                    LOG.warn("decoder is null");
                    return values;
                }
                byteArray = base64Decoder.decode(data.getPayload().getBytes());
            } else if (null != data.getHexPayload()) {
                LOG.debug(device.getEUI() + " hexPayload: " + data.getHexPayload());
                byteArray = getByteArray(data.getHexPayload());
            } else {
                LOG.debug(device.getEUI() + " payload is null");
                byteArray = emptyBytes;
            }
            if (null == byteArray) {
                byteArray = emptyBytes;
            }
            LOG.debug(device.getEUI() + " byteArray: " + Arrays.toString(byteArray));
            try {
                values = scriptingAdapter.decodeData(byteArray, device, application, data.getTimestamp());
            } catch (ScriptAdapterException ex) {
                ex.printStackTrace();
                addNotifications(device, null, ex.getMessage(), false);
                values = new ArrayList<>();
            } catch (Exception e) {
                e.printStackTrace();
                addNotifications(device, null, e.getMessage(), false);
                values = new ArrayList<>();
            }
        }
        if (!data.getDataList().isEmpty()) {
            for (int i = 0; i < data.getDataList().size(); i++) {
                values.add(data.getDataList().get(i));
            }
        }
        return values;
    }

    private void addNotifications(Device device, IotEvent event, String errorMessage, boolean withMessage) {
        int alertLevel = 0;
        // INFO, ALERT and WARNING notifications are saved as signals and sentinel
        // events
        if (event.getType() == IotEvent.ALERT) {
            alertLevel = 3;
        } else if (event.getType() == IotEvent.WARNING) {
            alertLevel = 2;
        } else { // INFO
            alertLevel = 1;
        }

        HashSet<String> recipients = new HashSet<>();
        recipients.add(device.getUserID());
        if (device.getTeam() != null) {
            String[] r = device.getTeam().split(",");
            for (int j = 0; j < r.length; j++) {
                if (!r[j].isEmpty()) {
                    recipients.add(r[j]);
                }
            }
        }
        if (device.getAdministrators() != null) {
            String[] r = device.getAdministrators().split(",");
            for (int j = 0; j < r.length; j++) {
                if (!r[j].isEmpty()) {
                    recipients.add(r[j]);
                }
            }
        }
        IotEvent errEvent = null;
        if (null != errorMessage) {
            errEvent = new IotEvent("info", errorMessage);
        }

        Iterator itr = recipients.iterator();
        String userId;
        while (itr.hasNext()) {
            userId = (String) itr.next();
            if (null != event) {
                event.setOrigin(userId + "\t" + device.getEUI());
                // Because this kind of notification is not created by sentinel, there is no
                // sentinel event
                // associated with it and only the signal is saved
                Signal signal = new Signal();
                signal.deviceEui = device.getEUI();
                signal.level = alertLevel;
                signal.messageEn = (String) event.getPayload();
                signal.messagePl = (String) event.getPayload();
                signal.sentinelConfigId = -1L;
                signal.userId = userId;
                signal.createdAt = new Timestamp(event.getCreatedAt());
                signal.organizationId = device.getOrganizationId();
                try {
                    signalDao.saveSignal(signal);
                } catch (IotDatabaseException e) {
                    e.printStackTrace();
                }
                // }
                sendAlert(event.getType(), userId, device.getEUI(), (String) event.getPayload(),
                        (String) event.getPayload(), event.getCreatedAt(), withMessage);
            }
        }
        if (null != errEvent) {
            // error message is sent to device owner and administrators
            recipients.clear();
            recipients.add(device.getUserID());
            if (device.getAdministrators() != null) {
                String[] r = device.getAdministrators().split(",");
                for (int j = 0; j < r.length; j++) {
                    if (!r[j].isEmpty()) {
                        recipients.add(r[j]);
                    }
                }
            }
            itr = recipients.iterator();
            while (itr.hasNext()) {
                userId = (String) itr.next();
                sendAlert(errEvent.getType(), userId, device.getEUI(), "info", errorMessage,
                        System.currentTimeMillis(),
                        withMessage);
            }
        }
    }

    private void sendAlert(String alertType, String userId, String deviceEui, String alertSubject, String alertMessage,
            long createdAt, boolean withMessage) {
        LOG.debug("Sending alert to userId: " + userId);
        if (!withMessage) {
            return;
        }
        LOG.debug("Emitting and alert to userId: " + userId);
        alertEmitter.send(userId + "\t" + deviceEui + "\t" + alertType + "\t" + alertMessage + "\t" + alertSubject);
    }

    private byte[] getByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
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

    public Device getDeviceChecked(String eui, String authKey, boolean authRequired, DeviceType[] expectedTypes) {
        Device gateway = null;
        Device device = getDevice(eui);
        if (null == device) {
            LOG.warn("Device " + eui + " is not registered");
            return null;
        }
        if (authRequired) {
            String secret;
            //if (gateway == null) {
                secret = device.getKey();
            //} else {
            //    secret = gateway.getKey();
            //}
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
        if (useProtectedFeature) {
            // check if device is protected
            LOG.info("Checking if device is protected");
            String tagValue;
            try {
                tagValue = dao.getDeviceTagValue(device.getEUI(), "protected");
                LOG.info("Protected tag value: " + tagValue);
                device.setDataProtected(Boolean.parseBoolean(tagValue));
            } catch (IotDatabaseException e) {
                e.printStackTrace();
                LOG.error(e.getMessage());
            }
        }else{
            LOG.info("Protected feature is disabled");
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
