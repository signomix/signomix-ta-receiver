package com.signomix.receiver;

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
import com.signomix.common.iot.ttn3.TtnData3;
import com.signomix.common.iot.tts.RxMetadata;
import com.signomix.common.iot.virtual.VirtualData;
import com.signomix.common.tsdb.ApplicationDao;
import com.signomix.common.tsdb.IotDatabaseDao;
import com.signomix.common.tsdb.SignalDao;
import com.signomix.receiver.application.exception.ReceiverException;
import com.signomix.receiver.domain.helpers.BulkDataLoader;
import com.signomix.receiver.domain.helpers.BulkLoaderResult;
import com.signomix.receiver.domain.helpers.DelayFilterService;
import com.signomix.receiver.processor.DataProcessorIface;
import com.signomix.receiver.processor.DefaultProcessor;
import com.signomix.receiver.processor.NashornDataProcessor;
import com.signomix.receiver.processor.ProcessorResult;
import com.signomix.receiver.script.NashornScriptingAdapter;
import com.signomix.receiver.script.ScriptAdapterException;
import io.agroal.api.AgroalDataSource;
import io.quarkus.agroal.DataSource;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.sql.Timestamp;
import java.time.Instant;
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
import org.jboss.resteasy.reactive.server.multipart.MultipartFormDataInput;

@ApplicationScoped
public class ReceiverService {

    private static final DeviceType[] DEVICE_TYPES = {
        DeviceType.GENERIC,
        DeviceType.VIRTUAL,
        DeviceType.TTN,
        DeviceType.CHIRPSTACK,
        DeviceType.LORA,
    };

    static final String MAX_DELAY_PARAM_NAME = "maxDelay"; // eg. {"maxDelay": 30000} - 30 second data timestamp delay is allowed for the device
    static final long DELAY_SHIFT = 5_000L; // Arbitrarily chosen value of 5 seconds

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
    DelayFilterService delayFilterService;

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

    @Inject
    ObjectMapper objectMapper;

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

    @ConfigProperty(name = "frame.counter.cache.size", defaultValue = "10000")
    int frameCounterCacheSize;

    @ConfigProperty(
        name = "frame.counter.cleanup.interval.ms",
        defaultValue = "3600000"
    )
    long frameCounterCleanupIntervalMs;

    private ConcurrentHashMap<String, Long> frameCountersMap;
    private long lastFrameCounterCleanupTime = 0;

    /**
     * Cleans up frame counters map if it exceeds maximum size or cleanup interval has passed.
     * Removes 20% of oldest entries to prevent memory leak.
     */
    private void cleanupFrameCountersIfNeeded() {
        long now = System.currentTimeMillis();

        // Check if cleanup interval has passed or map is too large
        if (
            now - lastFrameCounterCleanupTime > frameCounterCleanupIntervalMs ||
            frameCountersMap.size() > frameCounterCacheSize
        ) {
            int entriesToRemove = Math.max(
                1,
                (int) (frameCountersMap.size() * 0.2)
            );

            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Cleaning up frame counters map. Current size: " +
                        frameCountersMap.size() +
                        ", removing: " +
                        entriesToRemove
                );
            }

            // Remove oldest entries (simple approach - remove first N entries)
            frameCountersMap
                .keySet()
                .stream()
                .limit(entriesToRemove)
                .forEach(frameCountersMap::remove);

            lastFrameCounterCleanupTime = now;
        }
    }

    public void onApplicationStart(@Observes StartupEvent event) {
        dao.setDatasource(tsDs);
        olapDao.setDatasource(tsDs);
        olapDao.setAnalyticDatasource(olapDs);
        signalDao.setDatasource(tsDs);
        appDao.setDatasource(tsDs);
        frameCountersMap = new ConcurrentHashMap<>();
        lastFrameCounterCleanupTime = System.currentTimeMillis();
    }

    public String processDataAndReturnResponse(IotData2 data) {
        return processData(data);
    }

    public BulkLoaderResult processCsv(
        Device device,
        MultipartFormDataInput input,
        boolean singleDevice
    ) {
        return bulkDataLoader.loadBulkData(
            device,
            olapDao,
            input,
            singleDevice
        );
    }

    public BulkLoaderResult processCsvString(Device device, String input) {
        return bulkDataLoader.loadBulkData(device, olapDao, input);
    }

    @ConsumeEvent(value = "iotdata-no-response")
    public void processDataNoResponse(IotData2 data) {
        processData(data);
    }

    @ConsumeEvent(value = "ttndata-no-response")
    void processTtnData(IotData2 data) {
        processData(data);
    }

    @ConsumeEvent(value = "ttndata3-no-response")
    void processTtnDataString(String dataString) {
        int atSignIndex = dataString.indexOf("@");
        String authKey = dataString.substring(0, atSignIndex);
        String jsonString = dataString.substring(atSignIndex + 1);
        Device device;
        try {
            TtnData3 dataObject = com.signomix.common.iot.tts.Decoder.decode(
                jsonString
            );
            device = getDeviceChecked(
                dataObject.deviceEui,
                authKey,
                true,
                DEVICE_TYPES
            );
            if (device == null) {
                LOG.warn(
                    "Device not found or unauthorized: " + dataObject.deviceEui
                );
                return;
            }
            long maxDelay = 0;
            try {
                HashMap<String, Object> config = device.getConfigurationMap();
                if (config.get(MAX_DELAY_PARAM_NAME) != null) {
                    maxDelay = (long) config.get(MAX_DELAY_PARAM_NAME);
                }
            } catch (Exception e) {
                LOG.debug(
                    "Error reading maxDelay from device config: " +
                        e.getMessage()
                );
            }
            if (maxDelay > 0) {
                boolean delayAccepted = delayFilterService.isDelayAccepted(
                    dataObject,
                    DELAY_SHIFT,
                    maxDelay
                );
                if (!delayAccepted) {
                    LOG.warn(
                        "Data is too delayed for device: " +
                            dataObject.deviceEui +
                            ", maxDelay: " +
                            maxDelay
                    );
                    return;
                }
            }
            IotData2 iotData = transform(dataObject, authKey, true, jsonString);
            if (null == iotData) {
                LOG.warn("Error while reading the data");
                return;
            }

            processData(iotData);
        } catch (Exception e) {
            LOG.error("Error processing TTN data: " + e.getMessage(), e);
        }
    }

    @ConsumeEvent(value = "chirpstackdata-no-response")
    void processChirpstackData(IotData2 data) {
        try {
            processData(data);
        } catch (Exception e) {
            LOG.error(
                "Error processing Chirpstack data for device: " +
                    (data != null ? data.getDeviceEUI() : "null"),
                e
            );
        }
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
    private ProcessorResult callProcessorService(
        ArrayList<ChannelData> inputList,
        Device device,
        Application application,
        IotData2 iotData,
        String dataString
    ) throws Exception {
        // TODO
        String processorClassName = null;
        String script = clear(device.getCodeUnescaped());
        // class name is in the first not empty line of the script if it's form is like
        // "//class=package.ClassName;"
        if (!script.isEmpty()) {
            processorClassName = getClassName(script);
        } else if (
            processorClassName == null &&
            application != null &&
            application.code != null
        ) {
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
                    processor = (DataProcessorIface) clazz
                        .getDeclaredConstructor()
                        .newInstance();
                } catch (Exception e) {
                    throw new Exception(
                        "Failed to instantiate processor: " +
                            processorClassName,
                        e
                    );
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
            dataString,
            "",
            olapDao,
            iotData.port,
            iotData.chirpstackUplink,
            iotData.ttnUplink
        );
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
        Arrays.sort(
            parts,
            new Comparator<String>() {
                public int compare(String s1, String s2) {
                    String eui1 = s1.split(":")[0];
                    String eui2 = s2.split(":")[0];
                    return eui1.compareTo(eui2);
                }
            }
        );
        String tmpEui = "";
        String[] dataObj;
        HashMap<String, String> map;
        //IotData2 iotData = new IotData2(systemTimestamp);
        IotData2 iotData = new IotData2();
        iotData.payload_fields = new ArrayList<>();
        for (String part : parts) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("part: " + part);
            }
            dataObj = part.split(":");
            if (!tmpEui.equals(dataObj[0])) {
                // save previous iotData
                if (
                    iotData != null &&
                    iotData.dev_eui != null &&
                    iotData.dev_eui.length() > 0
                ) {
                    // LOG.info("PROCESSING DATA FROM EUI: " + iotData.dev_eui);
                    iotData.normalize();
                    iotData.setTimestampUTC(systemTimestamp);
                    processData(iotData);
                }
                tmpEui = dataObj[0];
                //iotData = new IotData2(systemTimestamp);
                iotData = new IotData2();
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
        if (
            iotData != null &&
            iotData.dev_eui != null &&
            iotData.dev_eui.length() > 0
        ) {
            // LOG.info("PROCESSING DATA FROM EUI: " + iotData.dev_eui);
            iotData.normalize();
            iotData.setTimestampUTC(systemTimestamp);
            processData(iotData);
        }
    }

    private String processData(IotData2 data) {
        if (data == null) {
            LOG.warn("processData called with null data");
            return null;
        }
        if (data.payload_fields == null) {
            data.payload_fields = new ArrayList<>();
        }
        LOG.info("DATA FROM EUI: " + data.getDeviceEUI());
        long systemTimestamp = System.currentTimeMillis();
        String result = "";
        DeviceType[] expected = {
            DeviceType.GENERIC,
            DeviceType.VIRTUAL,
            DeviceType.TTN,
            DeviceType.CHIRPSTACK,
            DeviceType.LORA,
        };
        Device device = getDeviceChecked(data, expected);
        if (null == device) {
            // TODO: result.setData(authMessage);
            return null;
        }
        // frame counter check
        if (
            device.isCheckFrames() &&
            (device.getType() == DeviceType.TTN.name() ||
                device.getType() == DeviceType.CHIRPSTACK.name() ||
                device.getType() == DeviceType.LORA.name())
        ) {
            cleanupFrameCountersIfNeeded();

            String deviceKey = device.getEUI();
            long previousFrame = frameCountersMap.getOrDefault(deviceKey, 0L);
            long currentFrame = data.counter;
            long resetLevel = frameCounterCacheSize / 10; // Use 10% of cache size as reset level
            if (previousFrame - currentFrame >= resetLevel) {
                previousFrame = 0L;
            }
            frameCountersMap.put(deviceKey, currentFrame);
            if (currentFrame <= previousFrame) {
                LOG.warn(
                    "Frame counter error for device " +
                        deviceKey +
                        ": " +
                        currentFrame +
                        " <= " +
                        previousFrame
                );
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
            scriptResult = callProcessorService(
                inputList,
                device,
                app,
                data,
                dataString
            );
            if (null == scriptResult) {
                try {
                    scriptResult = getProcessingResult(
                        inputList,
                        device,
                        app,
                        data,
                        dataString
                    );
                } catch (Exception ex) {
                    LOG.warn("getProcessingResult failed", ex);
                    scriptResult = null;
                }
            }
            // data to save
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "scriptResult: " + serializeProcessorResult(scriptResult)
                );
                if (scriptResult != null && scriptResult.getOutput() != null) {
                    LOG.debug(
                        "outputList.size==" + scriptResult.getOutput().size()
                    );
                } else {
                    LOG.debug("outputList is null or scriptResult is null");
                }
            }
            if (scriptResult != null && scriptResult.getOutput() != null) {
                outputList = scriptResult.getOutput();
            } else {
                outputList = new ArrayList<>();
            }
            for (int i = 0; i < outputList.size(); i++) {
                saveData(device, outputList.get(i));
            }
            if (DeviceType.VIRTUAL.name().equals(device.getType())) {
                saveVirtualData(device, data);
            }
            // device status
            Double newDeviceStatus =
                scriptResult != null ? scriptResult.getDeviceState() : null;
            if (
                newDeviceStatus != null &&
                device.getState() != null &&
                device.getState().compareTo(newDeviceStatus) != 0
            ) {
                LOG.debug("updateDeviceStatus");
                updateDeviceStatus(
                    device.getEUI(),
                    device.getTransmissionInterval(),
                    newDeviceStatus,
                    device.ALERT_OK
                );
            } else if (device.isActive()) {
                LOG.debug("updateHealthStatus");
                updateHealthStatus(
                    device.getEUI(),
                    device.getTransmissionInterval(),
                    device.getState(),
                    device.ALERT_OK
                );
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                        "device: active " +
                            device.isActive() +
                            " status " +
                            device.getState() +
                            " script device status " +
                            newDeviceStatus
                    );
                }
            }
            statusUpdated = true;
        } catch (Exception e) {
            LOG.error(
                "Error processing data for device: " +
                    (device != null ? device.getEUI() : "null"),
                e
            );
        }
        if (!statusUpdated) {
            updateHealthStatus(
                device.getEUI(),
                device.getTransmissionInterval(),
                device.getState(),
                device.ALERT_OK
            );
        }
        if (null == scriptResult) {
            return "";
        }

        ArrayList<IotEvent> events =
            scriptResult != null && scriptResult.getEvents() != null
                ? scriptResult.getEvents()
                : new ArrayList<>();
        HashSet<String> commandTargets = new HashSet<>(); // list of devices to send commands

        // commands and notifications
        String targetEui;
        for (int i = 0; i < events.size(); i++) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("event " + i + " (" + device.getEUI() + ")");
            }
            if (
                IotEvent.ACTUATOR_CMD.equals(events.get(i).getType()) ||
                IotEvent.ACTUATOR_HEXCMD.equals(events.get(i).getType()) ||
                IotEvent.ACTUATOR_PLAINCMD.equals(events.get(i).getType())
            ) {
                // commands
                targetEui = saveCommand(events.get(i));
                if (null != targetEui) {
                    commandTargets.add(targetEui);
                }
            } else {
                // notifications
                addNotifications(
                    device,
                    (IotEvent) events.get(i).clone(),
                    null,
                    true
                );
            }
        }
        // data events
        if (!device.getType().equalsIgnoreCase(DeviceType.VIRTUAL.name())) {
            HashMap<String, ArrayList> dataEvents =
                scriptResult != null && scriptResult.getDataEvents() != null
                    ? scriptResult.getDataEvents()
                    : new HashMap<>();
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
                        LOG.debug(
                            "SENDING DATA CREATED EVENT (" +
                                device.getEUI() +
                                "): " +
                                newEvent.getPayload()
                        );
                    }
                    // send event to mqtt
                    dataCreatedEmitter.send((String) newEvent.getPayload());
                    // send event to event bus
                    sentToEventBus(payload);
                }
            }
        }

        // are commands waiting?
        if (
            device.getType().equals(DeviceType.VIRTUAL.name()) ||
            device.getType().equals(DeviceType.GENERIC.name())
        ) {
            try {
                IotEvent command = (IotEvent) dao.getFirstCommand(
                    device.getEUI()
                );
                if (null != command) {
                    String commandPayload = (String) command.getPayload();
                    // remove port number from command payload (if exists) becourse it is not
                    // relevant
                    // for the device of type GENERIC (DIRECT)
                    if (commandPayload.indexOf("@@@") > 0) {
                        commandPayload = commandPayload.substring(
                            0,
                            commandPayload.indexOf("@@@")
                        );
                    }
                    if (IotEvent.ACTUATOR_HEXCMD.equals(command.getType())) {
                        String rawCmd = new String(
                            Base64.getEncoder().encode(
                                HexTool.hexStringToByteArray(commandPayload)
                            )
                        );
                        result = rawCmd;
                    } else {
                        result = commandPayload;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                            "COMMANDID/PAYLOAD (" +
                                device.getEUI() +
                                "):" +
                                command.getId() +
                                "/" +
                                commandPayload
                        );
                    }
                    dao.removeCommand(command.getId());
                    dao.putCommandLog(command.getOrigin(), command);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(
                            "COMMANDID/PAYLOAD (" +
                                device.getEUI() +
                                ") IS NULL"
                        );
                    }
                }
            } catch (IotDatabaseException e) {
                LOG.error(
                    "Failed to process command for device: " + device.getEUI(),
                    e
                );
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
        if (scriptResult == null) {
            return "";
        }
        try {
            return objectMapper.writeValueAsString(scriptResult);
        } catch (Exception e) {
            LOG.error("Failed to serialize ProcessorResult", e);
            return "";
        }
    }

    private Application getApplication(Long appId) {
        Application app = null;
        if (null == appId) {
            return null;
        }
        try {
            app = appDao.getApplication(appId.intValue());
        } catch (IotDatabaseException e) {
            LOG.warn("Failed to get application: " + appId, e);
        }
        return app;
    }

    private void sentToEventBus(String payload) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("sending to event bus: " + payload);
        }
        bus.send("virtualdata-no-response", payload);
    }

    private ProcessorResult getProcessingResult(
        ArrayList<ChannelData> inputList,
        Device device,
        Application application,
        IotData2 iotData,
        String dataString
    ) throws Exception {
        ProcessorResult result = processor.getProcessingResult(
            inputList,
            device,
            application,
            iotData.getReceivedPackageTimestamp(),
            iotData.getLatitude(),
            iotData.getLongitude(),
            iotData.getAltitude(),
            dataString,
            "",
            olapDao,
            iotData.port
        );
        result.setApplicationConfig(device.getApplicationConfig());
        return result;
    }

    ArrayList<ChannelData> fixValues(
        Device device,
        ArrayList<ChannelData> values
    ) {
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
            dao.putDeviceCommand(origin[1], commandEvent, false);
            commandCreatedEmitter.send(
                origin[1] + ";" + commandEvent.getPayload().toString()
            );
            return origin[1];
        } catch (IotDatabaseException e) {
            LOG.error("Failed to save command", e);
        } catch (Exception e) {
            LOG.error("Unexpected error saving command", e);
        }
        return null;
    }

    private void saveData(Device device, ArrayList<ChannelData> list) {
        if (device == null || list == null) {
            LOG.warn("saveData called with null parameters");
            return;
        }

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("saveData list.size():" + list.size());
            }

            dao.putData(device, fixValues(device, list));
            olapDao.saveAnalyticData(device, list);

            HashMap<String, Double> redisMap = new HashMap<>();
            list.forEach(cdata -> {
                redisMap.put(cdata.getName(), cdata.getValue());
            });

            emitter.send(buildDataReceivedMessage(device, list));
        } catch (IotDatabaseException e) {
            LOG.error(
                "Failed to save data for device: " +
                    (device != null ? device.getEUI() : "null"),
                e
            );
        } catch (Exception e) {
            LOG.error(
                "Unexpected error while saving data for device: " +
                    (device != null ? device.getEUI() : "null"),
                e
            );
        }
    }

    private String buildDataReceivedMessage(
        Device device,
        ArrayList<ChannelData> list
    ) {
        StringBuilder sb = new StringBuilder();
        // message header
        sb.append(device.getEUI())
            .append(",")
            .append(device.getOrganizationId())
            .append(",")
            .append(device.getName())
            .append(",")
            .append(device.getState())
            .append(",")
            .append(device.getAlertStatus())
            .append(",")
            .append(device.getLatitude())
            .append(",")
            .append(device.getLongitude())
            .append(",")
            .append(device.getAltitude())
            .append(",");
        // measurements
        ChannelData cd = list.get(0);
        sb.append(cd.getTimestamp());
        // HEADER_SIZE = 9
        for (int i = 0; i < list.size(); i++) {
            cd = list.get(i);
            if (cd.getValue() != null) {
                sb.append(",")
                    .append(cd.getName())
                    .append("=")
                    .append(cd.getValue());
            }
        }
        LOG.info("data-received message: " + sb.toString());
        return sb.toString();
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
                            value = Double.parseDouble(
                                (String) tmp.get("value")
                            );
                        } catch (Exception e1) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug(
                                    "unable to parse " +
                                        name +
                                        " value: " +
                                        tmp.get("value")
                                );
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
            LOG.error(
                "Failed to save virtual data for device: " +
                    (device != null ? device.getEUI() : "null"),
                e
            );
        }
    }

    /**
     * Updates device status in database.
     *
     * @param eui Device EUI
     * @param transmissionInterval Transmission interval
     * @param newStatus New status value
     * @param newAlertStatus New alert status
     * @param statusType Type of status (for logging purposes)
     */
    private void updateDeviceStatusInternal(
        String eui,
        long transmissionInterval,
        Double newStatus,
        int newAlertStatus,
        String statusType
    ) {
        if (!deviceStatusUpdateIntegrated) {
            LOG.debug(statusType + " update skipped.");
            return;
        }
        try {
            dao.updateDeviceStatus(
                eui,
                transmissionInterval,
                newStatus,
                newAlertStatus
            );
            LOG.debug(statusType + " updated.");
        } catch (IotDatabaseException e) {
            LOG.error(
                "Failed to update " + statusType + " for device: " + eui,
                e
            );
        }
    }

    private void updateDeviceStatus(
        String eui,
        long transmissionInterval,
        Double newStatus,
        int newAlertStatus
    ) {
        updateDeviceStatusInternal(
            eui,
            transmissionInterval,
            newStatus,
            newAlertStatus,
            "Device status"
        );
    }

    private void updateHealthStatus(
        String eui,
        long transmissionInterval,
        Double newStatus,
        int newAlertStatus
    ) {
        updateDeviceStatusInternal(
            eui,
            transmissionInterval,
            newStatus,
            newAlertStatus,
            "Device health status"
        );
    }

    private ArrayList<ChannelData> decodePayload(
        IotData2 data,
        Device device,
        Application application
    ) {
        if (null == device) {
            LOG.warn("device is null");
            return new ArrayList<>();
        }
        ArrayList<ChannelData> values = new ArrayList<>();
        byte[] emptyBytes = {};
        byte[] byteArray = null;
        String deviceDecoderScript = device.getEncoderUnescaped();
        if (
            (null == deviceDecoderScript ||
                deviceDecoderScript.trim().isEmpty()) &&
            null != application
        ) {
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
                    LOG.debug(
                        device.getEUI() + " hexPayload: " + data.getHexPayload()
                    );
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
                LOG.debug(
                    device.getEUI() +
                        " byteArray: " +
                        Arrays.toString(byteArray)
                );
            }
            try {
                values = scriptingAdapter.decodeData(
                    byteArray,
                    device.getEUI(),
                    deviceDecoderScript,
                    data.getTimestamp()
                );
            } catch (ScriptAdapterException ex) {
                LOG.error(
                    "Script decoding failed for device: " + device.getEUI(),
                    ex
                );
                addNotifications(device, null, ex.getMessage(), false);
                values = new ArrayList<>();
            } catch (Exception e) {
                LOG.error(
                    "Unexpected error during decoding for device: " +
                        device.getEUI(),
                    e
                );
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

    private void addNotifications(
        Device device,
        IotEvent event,
        String errorMessage,
        boolean withMessage
    ) {
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
        } else {
            // INFO
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
                    LOG.error("Failed to save signal for user: " + userId, e);
                }
                // }
                sendAlert(
                    event.getType(),
                    userId,
                    device.getEUI(),
                    (String) event.getPayload(),
                    (String) event.getPayload(),
                    event.getCreatedAt(),
                    withMessage
                );
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
                sendAlert(
                    errEvent.getType(),
                    userId,
                    device.getEUI(),
                    "info",
                    errorMessage,
                    System.currentTimeMillis(),
                    withMessage
                );
            }
        }
    }

    private void sendAlert(
        String alertType,
        String userId,
        String deviceEui,
        String alertSubject,
        String alertMessage,
        long createdAt,
        boolean withMessage
    ) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending alert to userId: " + userId);
        }
        if (!withMessage) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Emitting and alert to userId: " + userId);
        }
        alertEmitter.send(
            userId +
                "\t" +
                deviceEui +
                "\t" +
                alertType +
                "\t" +
                alertMessage +
                "\t" +
                alertSubject
        );
    }

    private byte[] getByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) +
                Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    public Device getDevice(String eui) {
        LOG.debug("getDevice");
        Device device = null;
        // Device gateway = null;
        try {
            device = dao.getDevice(eui, true, true);
        } catch (IotDatabaseException e) {
            LOG.error("Failed to get device: " + eui, e);
        }
        return device;
    }

    public Device getDeviceChecked(
        String eui,
        String authKey,
        boolean authRequired,
        DeviceType[] expectedTypes
    ) {
        Device gateway = null;
        Device device = getDevice(eui);
        if (null == device) {
            LOG.warn("Device " + eui + " is not registered");
            return null;
        }
        if (authRequired) {
            String secret;
            secret = device.getKey();
            try {
                if (null == authKey || !authKey.equals(secret)) {
                    LOG.warn(
                        "Authorization key don't match for " +
                            device.getEUI() +
                            " :" +
                            authKey +
                            ":" +
                            secret
                    );
                    return null;
                }
            } catch (Exception ex) {
                LOG.warn("Authorization check failed for device: " + eui, ex);
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
                LOG.error(
                    "Failed to get protected tag for device: " +
                        device.getEUI(),
                    e
                );
            }
        } else {
            LOG.debug("Protected feature is disabled");
        }
        return device;
    }

    private Device getDeviceChecked(IotData2 data, DeviceType[] expectedTypes) {
        return getDeviceChecked(
            data.getDeviceEUI(),
            data.getAuthKey(),
            data.authRequired,
            expectedTypes
        );
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

    /*
    private boolean isDelayAccepted(
        TtnData3 dataObject,
        long maxDelay,
        long delayLimit
    ) {
        if (dataObject == null || dataObject.rxMetadata == null) {
            return false;
        }
        boolean delayed = false;
        long receivedTimestamp = dataObject.receivedAt;
        long start = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();

        long upperBound = receivedTimestamp + delayLimit;
        Long maxTimestamp = null;

        for (RxMetadata metadata : dataObject.rxMetadata) {
            if (metadata == null || metadata.getTime() == null) {
                continue;
            }
            long ta = metadata.getTime().getTime();
            if (
                ta > start &&
                ta <= upperBound &&
                (maxTimestamp == null || ta > maxTimestamp)
            ) {
                maxTimestamp = ta;
            }
        }
        if (maxTimestamp == null) {
            // for simulated uplinks
            return true;
        }
        delayed = receivedTimestamp - maxTimestamp > maxDelay;
        if (delayed) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    dataObject.deviceEui +
                        " data is too delayed, receivedAt: " +
                        dataObject.receivedAt +
                        ", maxTimestamp: " +
                        maxTimestamp
                );
            }
            return false;
        } else {
            return true;
        }
    }
    */

    private IotData2 transform(
        TtnData3 dataObject,
        String authKey,
        boolean authRequired,
        String jsonString
    ) throws ReceiverException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("transform " + authKey + " " + authRequired);
        }
        long systemTimestamp = System.currentTimeMillis();
        IotData2 data = new IotData2(systemTimestamp);
        data.dev_eui = dataObject.deviceEui;
        data.gateway_eui = null;
        data.timestamp = "" + dataObject.getTimestamp();

        data.clientname = "";
        data.authKey = authKey;
        data.authRequired = authRequired;
        data.port = dataObject.getPort();
        data.counter = dataObject.getFrameCounter();
        data.timestampUTC = new Timestamp(dataObject.timestamp);
        data.payload_fields = new ArrayList<>();
        HashMap pfMap = dataObject.getPayloadFields();
        // Data channel names should be lowercase. We can fix user mistakes here.
        HashMap<String, Object> tempMap;
        Iterator<String> it = pfMap.keySet().iterator();
        String key;
        while (it.hasNext()) {
            tempMap = new HashMap<>();
            key = it.next();
            tempMap.put("name", key.toLowerCase());
            Object value = pfMap.get(key);
            if (value == null) {
                LOG.warn("Null value for key: " + key);
                continue; // Skip null values
            }
            if (value instanceof Number) {
                tempMap.put("value", ((Number) value).doubleValue());
            } else if (value instanceof Boolean) {
                tempMap.put("value", (Boolean) value ? 1.0 : 0.0);
            } else if (value instanceof String) {
                tempMap.put("value", value);
            } else {
                LOG.warn(
                    "Unsupported value type for key: " +
                        key +
                        ", value: " +
                        value
                );
            }
            data.payload_fields.add(tempMap);
        }
        data.normalize();
        data.setTimestampUTC(systemTimestamp);
        return data;
    }
}
