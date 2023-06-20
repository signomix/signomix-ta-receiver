package com.signomix.receiver;

import java.util.ArrayList;

import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;

public class BulkDataLoader {
    
    public BulkDataLoader() {
    }

    public String loadBulkData(Device device, IotDatabaseIface dao, MultipartFormDataInput input) {
        long counter=0;
        ArrayList<ChannelData> data = new ArrayList<>();
        ChannelData cd = new ChannelData();
        cd.setDeviceEUI(device.getEUI());
        cd.setName("temp");
        cd.setTimestamp(0);
        cd.setValue(0.0);
        data.add(cd);
        try {
            dao.putData(device,data);
        } catch (IotDatabaseException e) {
            e.printStackTrace();
        }
        return "Device EUI "+device.getEUI()+" - records loaded: "+counter+"\n";
    }
}
