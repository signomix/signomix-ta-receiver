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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.signomix.receiver.processor.DataProcessorIface;
import com.signomix.receiver.processor.DefaultProcessor;
import com.signomix.receiver.processor.NashornDataProcessor;
import com.signomix.receiver.processor.ProcessorResult;
import com.signomix.receiver.script.NashornScriptingAdapter;
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
    NashornDataProcessor processor;

    @Inject
    NashornScriptingAdapter scriptingAdapter;

    @Inject
    BulkDataLoader bulkDataLoader;

    @Inject
    @Channel("data-received")
    Emitter<String> emitter;
    @Inject
    @Channel("command-created")
    Emitter<String> commandCreatedEmitter;
    @Inject
    @Channel("data-created")
    Emitter<String> dataCreatedEmitter;
    @Inject
    @Channel("alerts")
    Emitter<String> alertEmitter;
    @Inject
    @Channel("command-ready")
    Emitter<String> commandEmitter;

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

    private ConcurrentHashMap<String, Long> frameCountersMap;

    public void onApplicationStart(@Observes StartupEvent event) {
        dao.setDatasource(tsDs);
        olapDao.setDatasource(tsDs);
        olapDao.setAnalyticDatasource(olapDs);
        signalDao.setDatasource(tsDs);
        appDao.setDatasource(tsDs);
        frameCountersMap = new ConcurrentHashMap<>();
    }

    public String processDataAndReturnResponse(IotData2 data) {
        return processData(data);
    }

    public BulkLoaderResult processCsv(Device device, MultipartFormDataInput input, boolean singleDevice) {
        // if (null != dao) {
        return bulkDataLoader.loadBulkData(device, olapDao, input, singleDevice);
        // }
        // return null;
    }

    public BulkLoaderResult processCsvString(Device device, String input) {
        // if (null != dao) {
        return bulkDataLoader.loadBulkData(device, olapDao, input);
        // }
        // return null;
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("virtualdata-no-response: " + payload);
        }
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
            Application application, IotData2 iotData, String dataString) throws Exception {
        // TODO
        String processorClassName = null;
        String script = clear(device.getCodeUnescaped());
        // class name is in the first not empty line of the script if it's form is like
        // "//class=package.ClassName;"
        if (!script.isEmpty()) {
            processorClassName = getClassName(script);
        } else if (processorClassName == null && application != null && application.code != null) {
            script = clear(application.code);
        }

        DataProcessorIface processor = null;
        if (script.isEmpty()) {
            processor = new DefaultProcessor();
        } else {
            processorClassName = getClassName(script);
            if (processorClassName != null && !processorClassName.isEmpty()) {
                // Instantiate the processor using the processorClassName
                try {
                    Class<?> clazz = Class.forName(processorClassName);
                    processor = (DataProcessorIface) clazz.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    throw new Exception("Failed to instantiate processor: " + processorClassName, e);
                }
            }
        }
        if (processor == null) {
            return null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("processorClassName: " + processor.getClass().getName());
        }

        ProcessorResult result = processor.getProcessingResult(
                inputList,
                device,
                application,
                iotData.getReceivedPackageTimestamp(),
                iotData.getLatitude(),
                iotData.getLongitude(),
                iotData.getAltitude(),
                dataString, "",
                olapDao, iotData.port,
                iotData.chirpstackUplink,
                iotData.ttnUplink);
        if (result != null) {
            result.setApplicationConfig(device.getApplicationConfig());
        }

        return result;
    }

    private String clear(String code) {
        if (code == null || code.isEmpty()) {
            return "";
        }
        String script = code;
        // remove all empty lines or lines with only whitespaces
        script = script.replaceAll("(?m)^[ \t]*\r?\n", "");
        script = script.replaceAll("(?m)^[ \t]*$", "");
        // remove all leading and trailing whitespaces
        return script.trim();
    }

    private String getClassName(String deviceScript) {
        String processorClassName = null;
        String[] lines = deviceScript.split("\n");
        for (String line : lines) {
            if (line.trim().isEmpty()) {
                continue;
            }
            if (line.startsWith("//class=")) {
                processorClassName = line.substring(8).trim();
                break;
            }
        }
        return processorClassName;
    }

    private void parseBusMessage(String payload) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("parseBusMessage: " + payload);
        }
        long systemTimestamp = System.currentTimeMillis();
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
        IotData2 iotData = new IotData2(systemTimestamp);
        iotData.payload_fields = new ArrayList<>();
        for (String part : parts) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("part: " + part);
            }
            dataObj = part.split(":");
            if (!tmpEui.equals(dataObj[0])) {
                // save previous iotData
                if (iotData != null && iotData.dev_eui != null && iotData.dev_eui.length() > 0) {
                    // LOG.info("PROCESSING DATA FROM EUI: " + iotData.dev_eui);
                    iotData.normalize();
                    iotData.setTimestampUTC(systemTimestamp);
                    processData(iotData);
                }
                tmpEui = dataObj[0];
                iotData = new IotData2(systemTimestamp);
                iotData.dev_eui = dataObj[0];
                if (dataObj.length > 3) {
                    iotData.timestamp = dataObj[3];
                } else {
                    iotData.timestamp = "" + systemTimestamp;
                }
                iotData.payload_fields = new ArrayList<>();
            }
            map = new HashMap<>();
            map.put("name", dataObj[1]);
            map.put("value", dataObj[2]);
            iotData.payload_fields.add(map);
        }
        if (iotData != null && iotData.dev_eui != null && iotData.dev_eui.length() > 0) {
            // LOG.info("PROCESSING DATA FROM EUI: " + iotData.dev_eui);
            iotData.normalize();
            iotData.setTimestampUTC(systemTimestamp);
            processData(iotData);
        }
    }

    private String processData(IotData2 data) {
        LOG.info("DATA FROM EUI: " + data.getDeviceEUI());
        /*
         * if (data.getDeviceEUI().startsWith("DKHSROOM")) {
         * ObjectMapper mapper = new ObjectMapper();
         * LOG.info("DATA: " + mapper.valueToTree(data).toString());
         * }
         */
        long systemTimestamp = System.currentTimeMillis();
        String result = "";
        DeviceType[] expected = { DeviceType.GENERIC, DeviceType.VIRTUAL, DeviceType.TTN, DeviceType.CHIRPSTACK,
                DeviceType.LORA };
        Device device = getDeviceChecked(data, expected);
        if (null == device) {
            // TODO: result.setData(authMessage);
            return null;
        }
        // frame counter check
        if (device.isCheckFrames()
                && (device.getType() == DeviceType.TTN.name()
                        || device.getType() == DeviceType.CHIRPSTACK.name()
                        || device.getType() == DeviceType.LORA.name())) {
            long previousFrame = frameCountersMap.getOrDefault(device, 0L);
            long currentFrame = data.counter;
            long resetLevel = 100L; // TODO: get from device
            if (previousFrame - currentFrame >= resetLevel) {
                previousFrame = 0L;
            }
            frameCountersMap.put(device.getEUI(), currentFrame);
            if (currentFrame <= previousFrame) {
                LOG.warn("Frame counter error: " + currentFrame + " <= " + previousFrame);
                // return "ERROR: Frame counter error: "
                // + currentFrame + " <= " + previousFrame;
            }

        }

        String parserError = getFirstParserErrorValue(data);
        if (null != parserError && !parserError.isEmpty()) {
            return "ERROR: " + parserError;
        }
        data.setTimestampUTC(systemTimestamp);
        data.prepareIotValues(systemTimestamp);
        Application app = getApplication(device.getOrgApplicationId());
        if (LOG.isDebugEnabled()) {
            if (null == app) {
                LOG.debug("app is null");
            } else {
                LOG.debug("app code: " + app.code);
            }
        }
        ArrayList<ChannelData> inputList = decodePayload(data, device, app);
        if (LOG.isDebugEnabled()) {
            for (int i = 0; i < inputList.size(); i++) {
                LOG.debug(inputList.get(i).toString());
            }
        }
        ProcessorResult scriptResult = null;
        ArrayList<ArrayList> outputList;
        String dataString = null;
        boolean statusUpdated = false;
        try {
            scriptResult = callProcessorService(inputList, device, app, data, dataString);
            // possible exception in callProcessorService is handled in the catch block
            if (null == scriptResult) {
                scriptResult = getProcessingResult(inputList, device, app, data, dataString);
            }
            // data to save
            if (LOG.isDebugEnabled()) {
                LOG.debug("scriptResult: " + serializeProcessorResult(scriptResult));
                LOG.debug("outputList.size()==" + scriptResult.getOutput().size());
            }
            outputList = scriptResult.getOutput();
            for (int i = 0; i < outputList.size(); i++) {
                saveData(device, outputList.get(i));
            }
            if (DeviceType.VIRTUAL.name().equals(device.getType())) {
                saveVirtualData(device, data);
            }
            // device status
            Double newDeviceStatus = scriptResult.getDeviceState();
            if (newDeviceStatus != null && device.getState().compareTo(newDeviceStatus) != 0) {
                LOG.debug("updateDeviceStatus");
                updateDeviceStatus(device.getEUI(), device.getTransmissionInterval(), newDeviceStatus,
                        device.ALERT_OK);
            } else if (device.isActive()) {
                Log.debug("updateHealthStatus");
                updateHealthStatus(device.getEUI(), device.getTransmissionInterval(), device.getState(),
                        device.ALERT_OK);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("device: active " + device.isActive() + " status " + device.getState()
                            + " script device status " + newDeviceStatus);
                }
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
        HashSet<String> commandTargets = new HashSet<>(); // list of devices to send commands

        // commands and notifications
        String targetEui;
        for (int i = 0; i < events.size(); i++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("event " + i + " (" + device.getEUI() + ")");
            }
            if (IotEvent.ACTUATOR_CMD.equals(events.get(i).getType())
                    || IotEvent.ACTUATOR_HEXCMD.equals(events.get(i).getType())
                    || IotEvent.ACTUATOR_PLAINCMD.equals(events.get(i).getType())) {
                // commands
                targetEui = saveCommand(events.get(i));
                if (null != targetEui) {
                    commandTargets.add(targetEui);
                }
            } else {
                // notifications
                addNotifications(device, (IotEvent) events.get(i).clone(), null, true);
            }
        }
        // data events
        if (!device.getType().equalsIgnoreCase(DeviceType.VIRTUAL.name())) {
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
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("SENDING DATA CREATED EVENT (" + device.getEUI() + "): " + newEvent.getPayload());
                    }
                    // send event to mqtt
                    dataCreatedEmitter.send((String) newEvent.getPayload());
                    // send event to event bus
                    sentToEventBus(payload);

                }
            }
        }

        // are commands waiting?
        if (device.getType().equals(DeviceType.VIRTUAL.name())
                || device.getType().equals(DeviceType.GENERIC.name())) {
            try {
                IotEvent command = (IotEvent) dao.getFirstCommand(device.getEUI());
                if (null != command) {
                    String commandPayload = (String) command.getPayload();
                    // remove port number from command payload (if exists) becourse it is not
                    // relevant
                    // for the device of type GENERIC (DIRECT)
                    if (commandPayload.indexOf("@@@") > 0) {
                        commandPayload = commandPayload.substring(0, commandPayload.indexOf("@@@"));
                    }
                    if (IotEvent.ACTUATOR_HEXCMD.equals(command.getType())) {
                        String rawCmd = new String(
                                Base64.getEncoder().encode(HexTool.hexStringToByteArray(commandPayload)));
                        result = rawCmd;
                    } else {
                        result = commandPayload;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("COMMANDID/PAYLOAD (" + device.getEUI() + "):" + command.getId() + "/"
                                + commandPayload);
                    }
                    dao.removeCommand(command.getId());
                    dao.putCommandLog(command.getOrigin(), command);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("COMMANDID/PAYLOAD (" + device.getEUI() + ") IS NULL");
                    }
                }
            } catch (IotDatabaseException e) {
                e.printStackTrace();
            }
        }
        // when commands has been created for LoRa devices, send info to message broker
        if (commandTargets.size() > 0) {
            for (String target : commandTargets) {
                commandEmitter.send(target);
            }
        }
        return result;
    }

    private String serializeProcessorResult(ProcessorResult scriptResult) {
        ObjectMapper mapper = new ObjectMapper();
        String result = "";
        try {
            result = mapper.writeValueAsString(scriptResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private Application getApplication(Long appId) {
        Application app = null;
        if (null == appId) {
            return null;
        }
        try {
            app = appDao.getApplication(appId.intValue());
        } catch (IotDatabaseException e) {
            LOG.warn(e.getMessage());
        }
        return app;
    }

    private void sentToEventBus(String payload) {
        // IotDataMessageCodec iotDataCodec = new IotDataMessageCodec();
        // DeliveryOptions options = new
        // DeliveryOptions().setCodecName(iotDataCodec.name());
        if (LOG.isDebugEnabled()) {
            LOG.debug("sending to event bus: " + payload);
        }
        bus.send("virtualdata-no-response", payload);
    }

    private ProcessorResult getProcessingResult(ArrayList<ChannelData> inputList, Device device,
            Application application, IotData2 iotData,
            String dataString)
            throws Exception {
        ProcessorResult result = processor.getProcessingResult(inputList, device, application,
                iotData.getReceivedPackageTimestamp(), iotData.getLatitude(),
                iotData.getLongitude(), iotData.getAltitude(), dataString, "", olapDao, iotData.port);
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

    private String saveCommand(IotEvent commandEvent) {
        try {
            String[] origin = commandEvent.getOrigin().split("@");
            if (LOG.isDebugEnabled()) {
                LOG.debug("saving command (" + origin[1] + ")");
            }
            IotEvent ev = commandEvent;
            dao.putDeviceCommand(origin[1], commandEvent);
            commandCreatedEmitter.send(origin[1] + ";" + commandEvent.getPayload().toString());
            return origin[1];
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void saveData(Device device, ArrayList<ChannelData> list) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("saveData list.size():" + list.size());
            }
            if (null != dao) {
                dao.putData(device, fixValues(device, list));
            }
            if (null != olapDao) {
                LOG.debug("saveData to olap DB");
                olapDao.saveAnalyticData(device, list);
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
            try {
                vd.timestamp = data.getTimestampUTC().getTime();
            } catch (NullPointerException e) {
                vd.timestamp = System.currentTimeMillis();
            }
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
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("unable to parse " + name + " value: " + tmp.get("value"));
                            }
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
            LOG.debug("Device status update skipped.");
            return;
        }
        try {
            if (null != dao) {
                dao.updateDeviceStatus(eui, transmissionInterval, newStatus, newAlertStatus);
            }
            LOG.debug("Device status updated.");
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            LOG.error(e.getMessage());
        }
    }

    private void updateHealthStatus(String eui, long transmissionInterval, Double newStatus, int newAlertStatus) {
        if (!deviceStatusUpdateIntegrated) {
            // TEST
            LOG.debug("Device health status update skipped.");
            return;
        }
        try {
            if (null != dao) {
                dao.updateDeviceStatus(eui, transmissionInterval, newStatus, newAlertStatus);
            }
            LOG.debug("Device health status updated.");
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
        String deviceDecoderScript = device.getEncoderUnescaped();
        if ((null == deviceDecoderScript || deviceDecoderScript.trim().isEmpty()) && null != application) {
            deviceDecoderScript = application.decoder;
        }
        if (null != deviceDecoderScript && deviceDecoderScript.length() > 0) {
            if (null != data.getPayload()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("base64Payload: " + data.getPayload());
                }
                Decoder base64Decoder = Base64.getDecoder();
                if (null == base64Decoder) {
                    LOG.warn("decoder is null");
                    return values;
                }
                byteArray = base64Decoder.decode(data.getPayload().getBytes());
            } else if (null != data.getHexPayload()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(device.getEUI() + " hexPayload: " + data.getHexPayload());
                }
                byteArray = getByteArray(data.getHexPayload());
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(device.getEUI() + " payload is null");
                }
                byteArray = emptyBytes;
            }
            if (null == byteArray) {
                byteArray = emptyBytes;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(device.getEUI() + " byteArray: " + Arrays.toString(byteArray));
            }
            try {
                // values = scriptingAdapter.decodeData(byteArray, device, application,
                // data.getTimestamp());
                values = scriptingAdapter.decodeData(byteArray, device.getEUI(), deviceDecoderScript,
                        data.getTimestamp());
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
        if (null == event) {
            LOG.warn("event is null");
            return;
        }
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
                String message = (String) event.getPayload();
                if (message.length() > 255) {
                    message = message.substring(0, 250) + " ...";
                }
                signal.messageEn = message;
                signal.messagePl = message;
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending alert to userId: " + userId);
        }
        if (!withMessage) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Emitting and alert to userId: " + userId);
        }
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
            device = dao.getDevice(eui, true, true);
            // gateway = getDevice(data.getGatewayEUI());
        } catch (IotDatabaseException e) {
            e.printStackTrace();
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
            // if (gateway == null) {
            secret = device.getKey();
            // } else {
            // secret = gateway.getKey();
            // }
            try {
                if (null == authKey || !authKey.equals(secret)) {
                    LOG.warn("Authorization key don't match for " + device.getEUI() + " :" + authKey + ":" + secret);
                    return null;
                }
            } catch (Exception ex) { // catch (UserException ex) {
                // ex.printStackTrace();
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
            LOG.debug("Checking if device is protected");
            String tagValue;
            try {
                tagValue = dao.getDeviceTagValue(device.getEUI(), "protected");
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Protected tag value: " + tagValue);
                }
                device.setDataProtected(Boolean.parseBoolean(tagValue));
            } catch (IotDatabaseException e) {
                e.printStackTrace();
                LOG.error(e.getMessage());
            }
        } else {
            LOG.debug("Protected feature is disabled");
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
