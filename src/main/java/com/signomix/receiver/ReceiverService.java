package com.signomix.receiver;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Base64;
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
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.DeviceType;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.common.iot.virtual.VirtualData;
import com.signomix.receiver.event.IotEvent;
import com.signomix.receiver.script.ProcessorResult;
import com.signomix.receiver.script.ScriptingAdapterIface;

import io.agroal.api.AgroalDataSource;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.vertx.ConsumeEvent;

@ApplicationScoped
public class ReceiverService {
    private static final Logger LOG = Logger.getLogger(ReceiverService.class);

    // TODO: test /q/health/ready

    @Inject
    AgroalDataSource ds;

    @Inject
    MessageService messageService;

    @Inject
    DataProcessor processor;

    ScriptingAdapterIface scriptingAdapter;
    IotDatabaseIface dao;

    @ConfigProperty(name = "signomix.app.key", defaultValue = "not_configured")
    String appKey;
    @ConfigProperty(name = "signomix.core.host", defaultValue = "not_configured")
    String coreHost;


    public void onApplicationStart(@Observes StartupEvent event) {
        dao = new IotDatabaseDao();
        dao.setDatasource(ds);
    }

    String processDataAndReturnResponse(IotData2 data) {
        return processData(data);
    }

    @ConsumeEvent(value = "iotdata-no-response")
    void processDataNoResponse(IotData2 data) {
        processData(data);
    }

    @ConsumeEvent(value = "ttndata-no-response")
    void processTtnData(IotData2 data) {
        processData(data);
    }
    /**
     * Sends data to dedicated microservice
     * @param inputList
     * @param device
     * @param iotData
     * @param dataString
     * @return data processing result
     */
    private ProcessorResult callProcessorService(ArrayList<ChannelData> inputList, Device device, IotData2 iotData,
            String dataString) {
                //TODO
        return null;
    }

    private String processData(IotData2 data) {
        LOG.info("DATA FROM EUI: " + data.getDeviceEUI());
        String result = "";
        DeviceType[] expected = { DeviceType.GENERIC, DeviceType.VIRTUAL };
        Device device = getDeviceChecked(data, expected);
        if (null == device) {
            // TODO: result.setData(authMessage);
            return result;
        }
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
                updateDeviceStatus(device.getEUI(), scriptResult.getDeviceState());
            } else if (device.isActive()) {
                updateHealthStatus(device.getEUI());
            }
            statusUpdated = true;
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: notification
        }
        if (!statusUpdated) {
            updateHealthStatus(device.getEUI());
        }
        if (null == scriptResult) {
            return "";
        }

        ArrayList<IotEvent> events = scriptResult.getEvents();
        HashMap<String, String> recipients;

        // commands and notifications
        for (int i = 0; i < events.size(); i++) {
            if (IotEvent.ACTUATOR_CMD.equals(events.get(i).getType())
                    || IotEvent.ACTUATOR_HEXCMD.equals(events.get(i).getType())
                    || IotEvent.ACTUATOR_PLAINCMD.equals(events.get(i).getType())) {
                // commands
                saveCommand(events.get(i));
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
                    try {
                        dao.addAlert(newEvent);
                    } catch (IotDatabaseException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
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
                LOG.debug("COMMANDID/PAYLOAD:"+command.getId()+"/"+commandPayload);
                dao.removeCommand(command.getId());
                dao.putCommandLog(command.getOrigin(), command);
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
            IotEvent ev=commandEvent;
            ev.setId(getNewCommandId(origin[1]));
            dao.putDeviceCommand(origin[0], commandEvent);
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }catch(Exception e){
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

    private void updateDeviceStatus(String eui, Double newStatus) {
        try {
            dao.updateDeviceStatus(eui, newStatus, System.currentTimeMillis(), -1, "", "");
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void updateHealthStatus(String eui) {
        try {
            dao.updateDeviceStatus(eui, null, System.currentTimeMillis(), -1, "", "");
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private ArrayList<ChannelData> decodePayload(IotData2 data, Device device) {
        if (!data.getDataList().isEmpty()) {
            LOG.debug("data list not empty");
            return data.getDataList();
        }
        ArrayList<ChannelData> values = new ArrayList<>();
        if (data.getPayloadFieldNames() == null || data.getPayloadFieldNames().length == 0) {
            if (null != data.getPayload()) {
                byte[] decodedPayload = Base64.getDecoder().decode(data.getPayload().getBytes());
                try {
                    values = scriptingAdapter.decodeData(decodedPayload, device, data.getTimestamp());
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        } else {
            LOG.debug("payloadFieldNamse not set");
            for (int i = 0; i < data.payload_fields.size(); i++) {
                HashMap map = (HashMap) data.payload_fields.get(i);

            }
        }
        return values;
    }

    public Device getDevice(String eui) {
        Device device = null;
        // Device gateway = null;
        try {
            device = dao.getDevice(eui);
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
                        .baseUri(new URI(coreHost+"/api/system/commandid"))
                        .followRedirects(true)
                        .build(CoreSystemService.class);
                return Long.parseLong(client.getNewCommandId(appKey,deviceEUI));
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


    private Device getDeviceChecked(IotData2 data, DeviceType[] expectedTypes) {
        Device device = null;
        Device gateway = null;
        try {
            device = dao.getDevice(data.getDeviceEUI());
            // gateway = getDevice(data.getGatewayEUI());
        } catch (IotDatabaseException e) {
            LOG.error(e.getMessage());
        }
        if (null == device) {
            LOG.warn("Device " + data.getDeviceEUI() + " is not registered");
            return null;
        }
        if (data.authRequired) {
            String secret;
            if (gateway == null) {
                secret = device.getKey();
            } else {
                secret = gateway.getKey();
            }
            try {
                if (!data.getAuthKey().equals(secret)) {
                    LOG.warn("Authorization key don't match for " + device.getEUI());
                    return null;
                }
            } catch (Exception ex) { // catch (UserException ex) {
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
            LOG.warn("Device " + data.getDeviceEUI() + " type is not valid");
            return null;
        }
        if (!device.isActive()) {
            // TODO: return "device is not active"?;
            return null;
        }
        return device;
    }

}
