package com.signomix.receiver;

import java.util.ArrayList;
import java.util.List;

import com.signomix.receiver.dto.ChannelData;
import com.signomix.receiver.dto.Device;

import io.agroal.api.AgroalDataSource;

public interface IotDatabaseIface {
    //ThinksAdapter
    public void setDatasource(AgroalDataSource ds);
    public void updateDeviceStatus(String eui, Double newStatus) throws IotDatabaseException;
    public void putData(Device device, ArrayList<ChannelData> list) throws IotDatabaseException;
    public Device getDevice(String eui) throws IotDatabaseException;
    public Device getDevice(String userID, String deviceEUI, boolean withShared) throws IotDatabaseException;
    public ChannelData getLastValue(String userID, String deviceID, String channel) throws IotDatabaseException;
    public List<List> getValues(String userID, String deviceID,String dataQuery)  throws IotDatabaseException;
    public List<String> getDeviceChannels(String deviceEUI) throws IotDatabaseException;
}
