package com.signomix.receiver;

import java.util.ArrayList;
import java.util.List;

import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.virtual.VirtualData;

import io.agroal.api.AgroalDataSource;

public interface IotDatabaseIface {
    //ThinksAdapter
    public void setDatasource(AgroalDataSource ds);
    public void updateDeviceStatus(String eui, Double newStatus, long timestamp, long lastFrame, String downlink, String deviceId) throws IotDatabaseException;
    public void putData(Device device, ArrayList<ChannelData> list) throws IotDatabaseException;
    public void putVirtualData(Device device, VirtualData data) throws IotDatabaseException;
    public Device getDevice(String eui) throws IotDatabaseException;
    public Device getDevice(String userID, String deviceEUI, boolean withShared) throws IotDatabaseException;
    public ChannelData getLastValue(String userID, String deviceID, String channel) throws IotDatabaseException;
    public List<List> getValues(String userID, String deviceID,String dataQuery)  throws IotDatabaseException;
    public List<String> getDeviceChannels(String deviceEUI) throws IotDatabaseException;
}
