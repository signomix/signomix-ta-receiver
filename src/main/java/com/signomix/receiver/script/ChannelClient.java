/**
 * Copyright (C) Grzegorz Skorupa 2018.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.receiver.script;

import java.util.List;

import com.signomix.common.iot.ChannelData;
import com.signomix.receiver.IotDatabaseException;
import com.signomix.receiver.IotDatabaseIface;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
public class ChannelClient {

    IotDatabaseIface thingsAdapter;
    String userID;
    String deviceID;

    public ChannelClient(String userID, String deviceID, IotDatabaseIface thingsAdapter) {
        this.thingsAdapter = thingsAdapter;
        this.userID = userID;
        this.deviceID = deviceID;
    }

    public ChannelData getLastData(String channel) {
        try {
            return thingsAdapter.getLastValue(userID, deviceID, channel);
        } catch (IotDatabaseException ex) {
            return null;
        }
    }

    public ChannelData getAverageValue(String channel, int scope) {
        return getAverageValue(channel, scope, null);
    }

    public ChannelData getAverageValue(String channel, int scope, Double newValue) {
        try {
            List<List> result;
            if (newValue == null) {
                result = thingsAdapter.getValues(userID, deviceID, "channel " + channel + " average " + scope);
            } else {
                result = thingsAdapter.getValues(userID, deviceID, "channel " + channel + " average " + scope + " new " + newValue);
            }
            if (result != null) {
                if (result.get(0).size() > 0) {
                    return (ChannelData) result.get(0).get(0);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ChannelData getMinimalValue(String channel, int scope) {
        return getMinimalValue(channel, scope, null);
    }

    public ChannelData getMinimalValue(String channel, int scope, Double newValue) {
        try {
            List<List> result;
            if (newValue == null) {
                result = thingsAdapter.getValues(userID, deviceID, "channel " + channel + " minimum " + scope);
            } else {
                result = thingsAdapter.getValues(userID, deviceID, "channel " + channel + " minimum " + scope + " new " + newValue);
            }
            if (result != null) {
                if (result.get(0).size() > 0) {
                    return (ChannelData) result.get(0).get(0);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ChannelData getMaximalValue(String channel, int scope) {
        return getMaximalValue(channel, scope, null);
    }

    public ChannelData getMaximalValue(String channel, int scope, Double newValue) {
        try {
            List<List> result;
            if (newValue == null) {
                result = thingsAdapter.getValues(userID, deviceID, "channel " + channel + " maximum " + scope);
            } else {
                result = thingsAdapter.getValues(userID, deviceID, "channel " + channel + " maximum " + scope + " new " + newValue);
            }
            if (result != null) {
                if (result.get(0).size() > 0) {
                    return (ChannelData) result.get(0).get(0);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public ChannelData getSummaryValue(String channel, int scope) {
        return getSummaryValue(channel, scope, null);
    }

    public ChannelData getSummaryValue(String channel, int scope, Double newValue) {
        try {
            List<List> result;
            if (newValue == null) {
                result = thingsAdapter.getValues(userID, deviceID, "channel " + channel + " sum " + scope);
            } else {
                result = thingsAdapter.getValues(userID, deviceID, "channel " + channel + " sum " + scope + " new " + newValue);
            }
            if (result != null) {
                if (result.get(0).size() > 0) {
                    return (ChannelData) result.get(0).get(0);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

}
