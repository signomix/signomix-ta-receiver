package com.signomix.common.db;

import java.util.ArrayList;
import java.util.List;

import com.signomix.common.event.IotEvent;
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
    public List<List> getLastValues(String userID, String deviceEUI) throws IotDatabaseException;
    public List<List> getValues(String userID, String deviceID,String dataQuery)  throws IotDatabaseException;
    public List<String> getDeviceChannels(String deviceEUI) throws IotDatabaseException;
    public IotEvent getFirstCommand(String deviceEUI) throws IotDatabaseException;
    public void removeCommand(long id) throws IotDatabaseException;
    public void putCommandLog(String deviceEUI, IotEvent command) throws IotDatabaseException;
    public void putDeviceCommand(String deviceEUI, IotEvent commandEvent) throws IotDatabaseException; 
    public long getMaxCommandId() throws IotDatabaseException;
    public long getMaxCommandId(String deviceEui) throws IotDatabaseException;
    //notifications
    public void addAlert(IotEvent alert) throws IotDatabaseException;
    public List getAlerts(String userID, boolean descending) throws IotDatabaseException;
    public void removeAlert(long alertID) throws IotDatabaseException;
    public void removeAlerts(String userID) throws IotDatabaseException;
    public void removeAlerts(String userID, long checkpoint) throws IotDatabaseException;
    public void removeOutdatedAlerts(long checkpoint) throws IotDatabaseException;
    public int getChannelIndex(String deviceEUI, String channel) throws IotDatabaseException;
}
