package com.signomix.receiver;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import com.signomix.common.DateTool;
import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;

@RequestScoped
public class BulkDataLoader {

    @Inject
    Logger logger;

    ArrayList<String> channels = new ArrayList<>();
    ArrayList<ChannelData> data = new ArrayList<>();
    Device device;
    IotDatabaseIface dao;
    IotDatabaseIface olapDao;
    boolean withEui = false; // first column in CSV line is device EUI
    BulkLoaderResult result = new BulkLoaderResult();
    int errors = 0;

    public BulkDataLoader() {
    }

    public BulkLoaderResult loadBulkData(Device loadedDevice, IotDatabaseIface dao, IotDatabaseIface olapDao, MultipartFormDataInput input) {
        this.dao = dao;
        device = loadedDevice;
        int lineNumber = 0;


        Map<String, List<InputPart>> uploadForm = input.getFormDataMap();
        List<InputPart> inputParts = uploadForm.get("file");

        String str = null;
        for (InputPart inputPart : inputParts) {
            try {
                InputStream inputStream = inputPart.getBody(InputStream.class, null);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                while ((str = reader.readLine()) != null) {
                    str=str.trim();
                    // skip empty lines and comments
                    if (str.isEmpty() || str.startsWith("#")) {
                        continue;
                    }
                    // process line
                    if (processLine(str, lineNumber)) {
                        lineNumber++;
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                errors++;
            }
        }
        result.errors = errors;
        result.loadedRecords = lineNumber-1;
        result.deviceEui = device.getEUI();
        return result;
    }

    private boolean processLine(String line, int lineNumber) {
        logger.debug(lineNumber + ": " + line);
        String[] parts = line.split(";");
        String channelName;
        if (lineNumber == 0) {
            // header line
            for (int i = 2; i < parts.length; i++) {
                channels.add(parts[i]);
            }
            // if header first column is "eui" then parser will expect device EUI in first column of each line
            if ("eui".equals(parts[0])) {
                withEui = true;
            }
            logger.debug("header: " + channels.toString());
            return true;
        } else {
            // data line
            int tstampPos = withEui ? 1 : 0;
            int firstValuePos = withEui ? 2 : 1;
            // check eui if needed
            if (withEui) {
                String deviceEUI = parts[0];
                if (!deviceEUI.equalsIgnoreCase(device.getEUI())) {
                    // invalid device EUI - ignore line
                    errors++;
                    return false;
                }
            }
            String timestampString = parts[tstampPos];
            long timestamp = getTimestamp(timestampString);
            logger.debug("timestamp: " + timestampString + " - " + timestamp);
            if (timestamp == 0) {
                // timestamp cannot be parsed - ignore line
                errors++;
                return false;
            }
            // parse values
            for (int i = firstValuePos; i < parts.length; i++) {
                channelName = channels.get(i - 2);
                ChannelData cd = new ChannelData();
                cd.setDeviceEUI(device.getEUI());
                cd.setName(channelName);
                cd.setTimestamp(timestamp);
                try {
                    cd.setValue(Double.parseDouble(parts[i]));
                } catch (NumberFormatException e) {
                    // value cannot be parsed - set null
                    errors++;
                    cd.setNullValue();
                }
                data.add(cd);
            }
            // line parsed - save data
            try {
                dao.putData(device, data);
            } catch (IotDatabaseException e) {
                logger.error(e.getMessage(), e);
                return false;
            }
            try {
                olapDao.putData(device, data);
            } catch (IotDatabaseException e) {
                logger.error(e.getMessage(), e);
                return false;
            }
            data.clear();
        }
        return true;
    }

    private long getTimestamp(String timestampString) {
        long timestamp = 0;
        try {
            timestamp = DateTool.parseTimestamp(timestampString, null, false).getTime();
        } catch (Exception e) {
            logger.error("invalid timestamp: " + timestampString);
        }
        return timestamp;
    }
}
