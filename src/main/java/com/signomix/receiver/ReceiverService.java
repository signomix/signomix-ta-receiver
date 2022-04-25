package com.signomix.receiver;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.generic.IotData;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.receiver.event.IotEvent;
import com.signomix.receiver.script.ScriptingAdapterIface;

import org.jboss.logging.Logger;

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

    public void onApplicationStart(@Observes StartupEvent event) {
        dao = new IotDatabaseDao();
        dao.setDatasource(ds);
    }

    public void test() {
        messageService.sendErrorInfo(new IotEvent());
    }

    @ConsumeEvent(value = "iotdata-no-response")
    void processData(IotData2 data) {
        LOG.debug("DATA FROM EUI: "+data.getDeviceEUI());
        Device device = getDeviceChecked(data, IotData.GENERIC);
        if (null == device) {
            // result.setData(authMessage);
            return;
        }
        data.prepareIotValues();
        ArrayList<ChannelData> inputList = decodePayload(data, device);
        for(int i=0; i< inputList.size(); i++){
            LOG.debug(inputList.get(i).toString());
        }
        ArrayList<ArrayList> outputList;
        //String dataString = data.getSerializedData();
        String dataString=null;
        boolean statusUpdated = false;
        try {
            Object[] processingResult = processValues(inputList, device, data, dataString);
            outputList = (ArrayList<ArrayList>) processingResult[0];
            for (int i = 0; i < outputList.size(); i++) {
                saveData(device, outputList.get(i));
            }
            if (device.isActive() && device.getState().compareTo((Double) processingResult[1]) != 0) {
                updateDeviceStatus(device.getEUI(), (Double) processingResult[1]);
            }else if(device.isActive()){
                updateHealthStatus(device.getEUI());
            }
            statusUpdated=true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(!statusUpdated){
            updateHealthStatus(device.getEUI());
        }
    }

    /*
    void processGenericRequest(IotData data) {
        IotData2 iotData = data.getIotData();
        HttpResult result = new HttpResult();
        result.code = 201;
        boolean htmlClient = false;
        String clientAppTitle = data.getClientName();
        if (null != clientAppTitle && !clientAppTitle.isEmpty()) {
            result.headers.put("Content-type", "text/html");
            htmlClient = true;
        }
        Device device = getDeviceChecked(data, IotData.GENERIC);
        if (null == device) {
            // result.setData(authMessage);
            return;
        }
        updateHealthStatus(device.getEUI());

        ArrayList<ChannelData> inputList = decodePayload(iotData, device);
        ArrayList<ArrayList> outputList;
        String dataString = data.getSerializedData();
        try {
            Object[] processingResult = processValues(inputList, device, iotData, dataString);
            outputList = (ArrayList<ArrayList>) processingResult[0];
            for (int i = 0; i < outputList.size(); i++) {
                saveData(device, outputList.get(i));
            }
            if (device.isActive() && device.getState().compareTo((Double) processingResult[1]) != 0) {
                updateDeviceStatus(device.getEUI(), (Double) processingResult[1]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        
        // Event command = ActuatorModule.getInstance().getCommand(device.getEUI(),
        // actuatorCommandsDB);
        // if (null != command) {
        // String commandPayload = (String) command.getPayload();
        // System.out.println("EVENT CATEGORY TYPE:" + command.getCategory() + " " +
        // command.getType());
        // if (IotEvent.ACTUATOR_HEXCMD.equals(command.getType())) {
        // String rawCmd = new
        // String(Base64.getEncoder().encode(HexTool.hexStringToByteArray(commandPayload
        // )));
        // result.setPayload(rawCmd.getBytes());
        // // TODO: odpowiedź jeśli dane z formularza
        // } else {
        // result.setPayload(commandPayload.getBytes());
        // // TODO: odpowiedź jeśli dane z formularza
        // }
        // ActuatorModule.getInstance().archiveCommand(command, actuatorCommandsDB);
        // }
        
        if (htmlClient) {
            result.code = 200;
            result.payload = buildResultData(htmlClient, true, clientAppTitle, "Data saved.");
        }
    }
    */

    private Object[] processValues(ArrayList<ChannelData> inputList, Device device, IotData2 iotData, String dataString)
            throws Exception {
        return processor.processValues(inputList, device,
                iotData.getReceivedPackageTimestamp(), iotData.getLatitude(),
                iotData.getLongitude(), iotData.getAltitude(), dataString, "", dao);
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

    private void saveData(Device device, ArrayList<ChannelData> list) {
        try {
            dao.putData(device, fixValues(device, list));
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void updateDeviceStatus(String eui, Double newStatus) {
        try {
            dao.updateDeviceStatus(eui, newStatus,System.currentTimeMillis(), -1, "", "");
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void updateHealthStatus(String eui) {
        try {
            dao.updateDeviceStatus(eui, null,System.currentTimeMillis(), -1, "", "");
        } catch (IotDatabaseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private String buildResultData(boolean html, boolean isSuccess, String title, String text) {
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
        }else{
            LOG.debug("payloadFieldNamse not set");
            for(int i=0;i<data.payload_fields.size(); i++){
                HashMap map=(HashMap)data.payload_fields.get(i);
                
            }
        }
        return values;
    }

    private Device getDevice(String eui) throws IotDatabaseException {
        return dao.getDevice(eui);
    }

    private Device getDeviceChecked(IotData2 data, int expectedType) {
        Device device = null;
        Device gateway = null;
        try {
            device = getDevice(data.getDeviceEUI());
            //gateway = getDevice(data.getGatewayEUI());
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
        
        switch (expectedType) {
            case IotData.GENERIC:
                if (!device.getType().startsWith("GENERIC") && !device.getType().startsWith("VIRTUAL")) {
                    LOG.warn("Device " + data.getDeviceEUI() + " type is not valid");
                    return null;
                }
                break;
            case IotData.CHIRPSTACK:
                if (!device.getType().startsWith("LORA")) {
                    LOG.warn("Device " + data.getDeviceEUI() + " type is not valid");
                    return null;
                }
                break;
            case IotData.TTN:
                if (!device.getType().startsWith("TTN")) {
                    LOG.warn("Device " + data.getDeviceEUI() + " type is not valid");
                    return null;
                }
                break;
            case IotData.KPN:
                if (!device.getType().startsWith("KPN")) {
                    LOG.warn("Device " + data.getDeviceEUI() + " type is not valid");
                    return null;
                }
                break;
        }
        if (!device.isActive()) {
            // return "device is not active";
            return null;
        }
        return device;
    }

}
