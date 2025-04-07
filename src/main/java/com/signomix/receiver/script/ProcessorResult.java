/**
 * Copyright (C) Grzegorz Skorupa 2018.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.receiver.script;

import com.signomix.common.event.IotEvent;
import com.signomix.common.iot.ChannelData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.jboss.logging.Logger;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
public class ProcessorResult {
    private static final Logger LOG = Logger.getLogger(ProcessorResult.class);

    HashMap<String, ChannelData> measures;
    HashMap<String, ArrayList> dataEvents;
    ArrayList<IotEvent> events;
    HashMap<String, Object> applicationConfig;

    public HashMap<String, Object> getApplicationConfig() {
        return applicationConfig;
    }

    public void setApplicationConfig(HashMap<String, Object> applicationConfig) {
        this.applicationConfig = applicationConfig;
    }

    /**
     * Each map in the output list can include data with different timestamps
     * This way, output can hold several data points
     */
    ListOfMaps output = new ListOfMaps();
    public Double deviceState;
    boolean listsUsed = false;

    public ProcessorResult() {
        dataEvents = new HashMap<>();
        measures = new HashMap<>();
        events = new ArrayList<>();
        output = new ListOfMaps();
    }

    public void log(String message) {
        LOG.info(message);
    }

    /* public void putData(ChannelData v) {
        measures.put(v.getName(), v);
    } */

    public void removeData(String channelName) {
        measures.remove(channelName);
    }

    public void rename(ChannelData v, String newName) {
        measures.remove(v.getName());
        v.setName(newName);
        measures.put(newName, v);
    }

    public void rename(String oldName, String newName) {
        ChannelData v = measures.get(oldName);
        if (null != v) {
            measures.remove(oldName);
            v.setName(newName);
            measures.put(newName, v);
        }
    }

    public void addEvent(String type, String message) {
        events.add(new IotEvent(type, message));
    }

    public void addDataEvent(String deviceName, String fromEui, ChannelData data) {
        String payload = deviceName + ":" + data.getName() + ":" + data.getValue() + ":" + data.getTimestamp();
        IotEvent event = new IotEvent(IotEvent.VIRTUAL_DATA, payload);
        event.setOrigin(fromEui); // to be informed which device created the event
        ArrayList<IotEvent> list = dataEvents.get(deviceName);
        if (null == list) {
            list = new ArrayList<>();
        }
        list.add(event);
        dataEvents.put(deviceName, list);
    }

    public void addCommand(String toDevice, String fromDevice, String payload, int representation, boolean overwrite) {
        int PLAIN_VER = 0;
        int HEX_VER = 1;
        int ENCODED_VER = 2;

        IotEvent event = new IotEvent();
        event.setOrigin(fromDevice + "@" + toDevice);
        if (HEX_VER == representation) {
            event.setType(IotEvent.ACTUATOR_HEXCMD);
        } else if (PLAIN_VER == representation) {
            event.setType(IotEvent.ACTUATOR_PLAINCMD);
        } else {
            event.setType(IotEvent.ACTUATOR_CMD);
        }
        String prefix;
        if (overwrite) {
            prefix = "#";
        } else {
            prefix = "&";
        }
        event.setPayload(prefix + payload);
        /*
         * if(ENCODED_VER!=representation){
         * event.setPayload(prefix+payload);
         * }else{
         * event.setPayload(prefix+Base64.getEncoder().encodeToString(payload.getBytes()
         * ));
         * }
         */
        events.add(event);
    }

    public void addGroupEvent(String groupEui, String fromEui, ChannelData data) {

        ArrayList<String> groupDevices = new ArrayList<>();

        for (String deviceEui : groupDevices) {
            if(fromEui.equalsIgnoreCase(deviceEui)){
                continue;
            }
            String payload = deviceEui + ":" + data.getName() + ":" + data.getValue() + ":" + data.getTimestamp();
            IotEvent event = new IotEvent(IotEvent.VIRTUAL_DATA, payload);
            event.setOrigin(fromEui); // to be informed which device created the event
            ArrayList<IotEvent> list = dataEvents.get(deviceEui);
            if (null == list) {
                list = new ArrayList<>();
            }
            list.add(event);
            dataEvents.put(deviceEui, list);
        }
    }

    public ArrayList<ChannelData> getMeasures() {
        ArrayList<ChannelData> result = new ArrayList<>();
        measures.keySet().forEach(key -> {
            result.add(measures.get(key));
        });
        return result;
    }

    public ArrayList<IotEvent> getEvents() {
        return events;
    }

    public HashMap<String, ArrayList> getDataEvents() {
        return dataEvents;
    }

    //////////////////// refactoring : new functions

    /**
     * Calculates distance between 2 points on Earth
     * 
     * @param lat1 latitude of the first point
     * @param lon1 longitude of the first
     * @param lat2 latitude of the second point
     * @param lon2 latitude of the second point
     * @return distance between points in meters
     */
    public long getDistance(double lat1, double lon1, double lat2, double lon2) {
        if ((lat1 == lat2) && (lon1 == lon2)) {
            return 0;
        } else {
            lon1 = Math.toRadians(lon1);
            lon2 = Math.toRadians(lon2);
            lat1 = Math.toRadians(lat1);
            lat2 = Math.toRadians(lat2);
            // Haversine formula
            double dlon = lon2 - lon1;
            double dlat = lat2 - lat1;
            double a = Math.pow(Math.sin(dlat / 2), 2)
                    + Math.cos(lat1) * Math.cos(lat2)
                            * Math.pow(Math.sin(dlon / 2), 2);
            double c = 2 * Math.asin(Math.sqrt(a));
            // Radius of earth in kilometers.
            double r = 6371;
            // result in meters
            return (long) (c * r * 1000);
        }
    }

    public long getModulo(long value, long divider) {
        return value % divider;
    }

    public long parseDate(String dateString) {
        return 0;
    }

    public void putData(String eui, String name, Object value, long timestamp, String stringValue) {
        Double v=null;
        if(value instanceof Double){
            v=(Double)value;
        }else if(value instanceof Integer){
            v=((Integer)value).doubleValue();
        }else if(value instanceof Long){
            v=((Long)value).doubleValue();
        }else if(value instanceof Float){
            v=((Float)value).doubleValue();
        }else if(value instanceof String){
            try{
                v=Double.parseDouble((String)value);
            }catch(Exception e){
                v=null;
            }
        }
        //if(v==null){
        //    return;
        //}
        output.put(new ChannelData(eui, name, v, timestamp, stringValue));
        listsUsed = true;
    }

    public void clearData() {
        output = new ListOfMaps();
        listsUsed = true;
    }

    /**
     * Returns actual hour based on current time zone
     * @param timezone example: "Europe/Warsaw"
     * @return hour in 24h format
     */
    public int getHour(String timezone) {
        java.util.Calendar cal = java.util.Calendar.getInstance();
        java.util.TimeZone tz = java.util.TimeZone.getTimeZone(timezone);
        cal.setTimeZone(tz);
        return cal.get(java.util.Calendar.HOUR_OF_DAY);
    }

    /**
     * Returns actual minute based on current time zone
     * @param timezone example: "Europe/Warsaw"
     * @return minute in 24h format
     */
    public int getMinute(String timezone) {
        java.util.Calendar cal = java.util.Calendar.getInstance();
        java.util.TimeZone tz = java.util.TimeZone.getTimeZone(timezone);
        cal.setTimeZone(tz);
        return cal.get(java.util.Calendar.MINUTE);
    }


    public ArrayList<ArrayList> getOutput() {
        if (listsUsed) {
            return output.getMeasures();
        } else {
            ArrayList<ArrayList> list = new ArrayList<>();
            list.add(getMeasures());
            return list;
        }
    }

    class DataMap extends HashMap<String, ChannelData> {

        private long timestamp = 0;

        DataMap(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }

    }

    class ListOfMaps {

        ArrayList<DataMap> maps = new ArrayList<>();

        protected void add(long timestamp) {
            maps.add(new DataMap(timestamp));
        }

        void put(ChannelData v) {
            for (int i = 0; i < maps.size(); i++) {
                if (v.getTimestamp() == maps.get(i).getTimestamp()) {
                    maps.get(i).put(v.getName(), v);
                    return;
                }
            }
            maps.add(new DataMap(v.getTimestamp()));
            put(v);
        }

        ArrayList<ArrayList> getMeasures() {
            ArrayList<ArrayList> result = new ArrayList<>();
            for (int i = 0; i < maps.size(); i++) {
                ArrayList<ChannelData> tmp = new ArrayList<>();
                Iterator it = maps.get(i).keySet().iterator();
                while (it.hasNext()) {
                    tmp.add(maps.get(i).get(it.next()));
                }
                result.add(tmp);
            }
            return result;
        }
    }

    /**
     * @return the deviceState
     */
    public Double getDeviceState() {
        return deviceState;
    }

    public Double getDeviceStatus() {
        return deviceState;
    }

    /**
     * @param deviceState the deviceState to set
     */
    public void setDeviceState(Double deviceState) {
        this.deviceState = deviceState;
    }

    public void setDeviceStatus(Double deviceStatus) {
        this.deviceState = deviceStatus;
    }
}
