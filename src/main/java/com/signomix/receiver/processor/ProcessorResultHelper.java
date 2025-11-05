package com.signomix.receiver.processor;

import java.util.ArrayList;

import org.jboss.logging.Logger;

import com.signomix.common.iot.ChannelData;

public class ProcessorResultHelper {

    private static Logger logger = Logger.getLogger(ProcessorResultHelper.class);

        /**
     * Calculates distance between 2 points on Earth
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
            return (long)(c * r * 1000);
        }
    }

    public long getModulo(long value, long divider) {
        return value % divider;
    }

    public long parseDate(String dateString) {
        return 0;
    }

    public void putData(ProcessorResult result, String eui, String name, Double value, long timestamp) {
        result.output.put(new ChannelData(eui, name, value, timestamp));
        result.listsUsed = true;
    }

    public ArrayList<ArrayList> getOutput(ProcessorResult result) {
        if (result.listsUsed) {
            return result.output.getMeasures();
        } else {
            ArrayList<ArrayList> list = new ArrayList<>();
            list.add(result.getMeasures());
            return list;
        }
    }

    public ArrayList<ChannelData> getMeasures(ProcessorResult result) {
        ArrayList<ChannelData> result2 = new ArrayList<>();
        result.measures.keySet().forEach(key -> {
            result2.add(result.measures.get(key));
        });
        return result2;
    }

    public void log(String level, String message) {
        switch (level) {
            case "debug":
                logger.debug(message);
                break;
            case "info":
                logger.info(message);
                break;
            case "warn":
                logger.warn(message);
                break;
            case "error":
                logger.error(message);
                break;
            default:
                logger.info(message);
        }
    }

    /**
     * Get date and time parts array from timestamp
     * @param timestamp
     * @param timeZone
     * @return
     */
    public String[] getDateTimeParts(long timestamp, String timeZone) {
        String[] parts = new String[6];
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTimeZone(java.util.TimeZone.getTimeZone(timeZone));
        cal.setTimeInMillis(timestamp);
        parts[0] = "" + cal.get(java.util.Calendar.DAY_OF_MONTH);
        parts[1] = "" + (cal.get(java.util.Calendar.MONTH) + 1);
        parts[2] = "" + cal.get(java.util.Calendar.YEAR);
        parts[3] = "" + cal.get(java.util.Calendar.HOUR_OF_DAY);
        parts[4] = "" + cal.get(java.util.Calendar.MINUTE);
        parts[5] = "" + cal.get(java.util.Calendar.SECOND);
        return parts;
    }

}
