package com.signomix.receiver;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.RequestScoped;

import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import com.signomix.common.DateTool;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;

@RequestScoped
public class BulkDataLoader {

    private ArrayList<String> channels = new ArrayList<>();
    private Device device;
    private IotDatabaseIface dao;
    ArrayList<ChannelData> data = new ArrayList<>();

    public BulkDataLoader() {
    }

    public String loadBulkData(Device device, IotDatabaseIface dao, MultipartFormDataInput input) {
        long lineNumber = 0;
        this.device = device;
        this.dao = dao;

        Map<String, List<InputPart>> uploadForm = input.getFormDataMap();
        // List<String> fileNames = new ArrayList<>();
        // String fileName = null;
        List<InputPart> inputParts = uploadForm.get("file");

        String str = null;
        for (InputPart inputPart : inputParts) {
            try {
                // MultivaluedMap<String, String> header = inputPart.getHeaders();
                // fileName = getFileName(header);
                // fileNames.add(fileName);
                InputStream inputStream = inputPart.getBody(InputStream.class, null);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                while ((str = reader.readLine()) != null) {
                    if (str.trim().isEmpty() || str.startsWith("#")) {
                        continue;
                    }
                    if(processLine(str, lineNumber)){
                        lineNumber++;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "Device EUI " + device.getEUI() + " - records loaded: " + (lineNumber - 1) + "\n";
    }

    private boolean processLine(String line, long lineNumber) {
        String[] parts = line.split(";");
        String channelName;
        if (lineNumber == 0) {
            // header
            for (int i = 2; i < parts.length; i++) {
                channels.add(parts[i]);
            }
            return false;
        } else {
            String deviceEUI = parts[0];
            if (!deviceEUI.equalsIgnoreCase(device.getEUI())) {
                // invalid device EUI
                return false;
            }
            String timestampString = parts[1];
            long timestamp = getTimestamp(timestampString);
            if (timestamp == 0) {
                // invalid timestamp
                return false;
            }
            for (int i = 2; i < parts.length; i++) {
                channelName = channels.get(i - 2);
                ChannelData cd = new ChannelData();
                cd.setDeviceEUI(device.getEUI());
                cd.setName(channelName);
                cd.setTimestamp(timestamp);
                try {
                    cd.setValue(Double.parseDouble(parts[i]));
                } catch (NumberFormatException e) {
                    // invalid value
                    cd.setNullValue();
                }
                data.add(cd);
                try {
                    dao.putData(device, data);
                } catch (IotDatabaseException e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    private long getTimestamp(String timestampString) {
        long timestamp = 0;
        try {
            timestamp = DateTool.parseTimestamp(timestampString,null,false).getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return timestamp;
    }
}
