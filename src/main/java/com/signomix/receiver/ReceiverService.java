package com.signomix.receiver;

import com.signomix.common.HexTool;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.event.IotEvent;
import com.signomix.common.event.MessageServiceIface;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.DeviceType;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.common.iot.sentinel.Signal;
import com.signomix.common.iot.virtual.VirtualData;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

@ApplicationScoped
public class ReceiverService {

    @Inject
    Logger LOG;

    // TODO: test /q/health/ready

    @Inject
    @DataSource("old")
    AgroalDataSource ds;

    @Inject
    @DataSource("oltp")
    AgroalDataSource tsDs;

    @Inject
    @DataSource("olap")
    AgroalDataSource olapDs;

    @Inject
    MessageServiceIface messageService;

    @Inject
    DataProcessor processor;

    @Inject
    NashornScriptingAdapter scriptingAdapter;
    // ScriptingAdapterIface scriptingAdapter;

    @Inject
    BulkDataLoader bulkDataLoader;

    @Inject
    @Channel("data-received")
    Emitter<String> emitter;
    @Inject
    @Channel("data-created")
    Emitter<String> dataCreatedEmitter;

    IotDatabaseIface dao = null;
    IotDatabaseIface tsDao = null;
    IotDatabaseIface olapDao = null;
    SignalDao signalDao = new SignalDao();

    private static AtomicLong commandIdSeed = null;
    private static AtomicLong eventSeed = new AtomicLong(System.currentTimeMillis());

    @Inject
    EventBus bus;

    /*
     * @ConfigProperty(name = "signomix.app.key", defaultValue = "not_configured")
     * String appKey;
     */
    /*
     * @ConfigProperty(name = "signomix.core.host", defaultValue = "not_configured")
     * String coreHost;
     */
    @ConfigProperty(name = "device.status.update.integrated")
    Boolean deviceStatusUpdateIntegrated;
    @ConfigProperty(name = "signomix.database.type")
    String databaseType;
    @ConfigProperty(name = "signomix.command_id.bytes", defaultValue = "0")
    Short commandIdBytes;
    @ConfigProperty(name = "signomix.signals.used", defaultValue = "false")
    Boolean signalsUsed;

    public void onApplicationStart(@Observes StartupEvent event) {
        if ("postgresql".equalsIgnoreCase(databaseType)) {
            LOG.info("using postgresql database");
            dao = new com.signomix.common.tsdb.IotDatabaseDao();
            olapDao = new com.signomix.common.tsdb.IotDatabaseDao();
            dao.setDatasource(tsDs);
            olapDao.setDatasource(tsDs);
            olapDao.setAnalyticDatasource(olapDs);
            return;
        } else if ("h2".equalsIgnoreCase(databaseType)) {
            LOG.info("using h2 database");
            dao = new com.signomix.common.db.IotDatabaseDao();
            dao.setDatasource(ds);
            return;
        } else if ("both".equalsIgnoreCase(databaseType)) {
            LOG.info("using both databases");
            dao = new com.signomix.common.db.IotDatabaseDao();
            dao.setDatasource(ds);
            tsDao = new com.signomix.common.tsdb.IotDatabaseDao();
            tsDao.setDatasource(tsDs);
            return;
        }
        signalDao.setDatasource(tsDs);
    }

    public String processDataAndReturnResponse(IotData2 data) {
        return processData(data);
    }

    public BulkLoaderResult processCsv(Device device, MultipartFormDataInput input) {
        if (null != dao) {
            return bulkDataLoader.loadBulkData(device, dao, olapDao, input);
        }
        if (null != tsDao) {
            return bulkDataLoader.loadBulkData(device, tsDao, olapDao, input);
        }
        return null;
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
        // String deviceId = data.deviceId;
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
            // addNotifications(device, null, e.getMessage(), false);
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

    private void sentToEventBus(String payload) {
        // IotDataMessageCodec iotDataCodec = new IotDataMessageCodec();
        // DeliveryOptions options = new
        // DeliveryOptions().setCodecName(iotDataCodec.name());
        LOG.debug("sending to event bus: " + payload);
        bus.send("virtualdata-no-response", payload);
    }

    private ProcessorResult getProcessingResult(ArrayList<ChannelData> inputList, Device device, IotData2 iotData,
            String dataString)
            throws Exception {
        ProcessorResult result = processor.getProcessingResult(inputList, device,
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
            // ev.setId(getNewCommandId(origin[1]));
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
            if (null != tsDao) {
                tsDao.putData(device, fixValues(device, list));
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
            if (null != tsDao) {
                tsDao.putVirtualData(device, vd);
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
            if (null != tsDao) {
                tsDao.updateDeviceStatus(eui, transmissionInterval, newStatus, newAlertStatus);
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
            if (null != tsDao) {
                tsDao.updateDeviceStatus(eui, transmissionInterval, newStatus, newAlertStatus);
            }
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
        ArrayList<ChannelData> values = new ArrayList<>();
        /*
         * if (!data.getDataList().isEmpty()) {
         * LOG.debug("data list not empty");
         * return data.getDataList();
         * }
         */

        byte[] emptyBytes = {};
        byte[] byteArray = null;
        String decoderScript = device.getEncoderUnescaped();
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
                values = scriptingAdapter.decodeData(byteArray, device, data.getTimestamp());
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
        IotEvent errEvent = null;
        if (null != errorMessage) {
            errEvent = new IotEvent("info", errorMessage);
        }
        Iterator itr = recipients.keySet().iterator();
        String userId;
        while (itr.hasNext()) {
            if (null != event) {
                try {
                    userId = (String) itr.next();
                    event.setOrigin(userId + "\t" + device.getEUI());
                    if (!signalsUsed) {
                        dao.addAlert(event);
                    } else {
                        if (event.getType() == IotEvent.GENERAL || event.getType() == IotEvent.INFO) {
                            // GENERAL and INFO notifications are saved as notifications (messages)
                            dao.addAlert(event);
                        } else {
                            // ALERT and WARNING notifications are saved as signals and sentinel events
                            int alertLevel = 0;
                            if (event.getType() == IotEvent.ALERT) {
                                alertLevel = 3;
                            } else if (event.getType() == IotEvent.WARNING) {
                                alertLevel = 2;
                            }
                            // Because this kind of notification is not created by sentinel, there is no
                            // sentinel event
                            // associated with it and only the signal is saved
                            Signal signal = new Signal();
                            signal.deviceEui = device.getEUI();
                            signal.level = alertLevel;
                            signal.messageEn = errorMessage;
                            signal.messagePl = errorMessage;
                            signal.sentinelConfigId = -1L;
                            signal.userId = userId;
                            signalDao.saveSignal(signal);
                        }
                    }
                } catch (IotDatabaseException e) {
                    e.printStackTrace();
                }
                if (withMessage) {
                    messageService.sendNotification(event);
                }
            }
            if (null != errEvent) {
                try {
                    //
                    errEvent.setOrigin(itr.next() + "\t" + device.getEUI());
                    // TODO: add alert
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

    /**
     * Returns next unique identifier for command (IotEvent).
     *
     * @return next unique identifier
     */
    /*
     * public synchronized long getNewCommandId(String deviceEui) {
     * // TODO: max value policy: 2,4,8 bytes (unsigned)
     * // default is long type (8 bytes)
     * if (commandIdBytes == 0) {
     * return eventSeed.getAndIncrement();
     * }
     * if (null == commandIdSeed) {
     * long seed = 0;
     * try {
     * if (null == deviceEui) {
     * seed = dao.getMaxCommandId();
     * } else {
     * seed = dao.getMaxCommandId(deviceEui);
     * }
     * } catch (IotDatabaseException e) {
     * LOG.warn(e.getMessage());
     * e.printStackTrace();
     * }
     * switch (commandIdBytes) {
     * case 2:
     * if (seed >= 65535L) {
     * seed = 0;
     * }
     * break;
     * case 4:
     * if (seed >= 4294967295L) {
     * seed = 0;
     * }
     * break;
     * default: // default per device ID is 8 bytes
     * if (seed == Long.MAX_VALUE) {
     * seed = 0;
     * }
     * }
     * commandIdSeed = new AtomicLong(seed);
     * }
     * long value = commandIdSeed.get();
     * long newValue;
     * switch (commandIdBytes) {
     * case 2:
     * if (value >= 65535L) {
     * newValue = 1;
     * } else {
     * newValue = value + 1;
     * }
     * break;
     * case 4:
     * if (value >= 4294967295L) {
     * newValue = 1;
     * } else {
     * newValue = value + 1;
     * }
     * break;
     * default:
     * if (value == Long.MAX_VALUE) {
     * newValue = 1;
     * } else {
     * newValue = value + 1;
     * }
     * }
     * commandIdSeed.set(newValue);
     * return newValue;
     * }
     */

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
