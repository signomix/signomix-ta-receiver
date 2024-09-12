/**
 * Copyright (C) Grzegorz Skorupa 2018.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.receiver.script;

import java.util.List;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.ChannelData;

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

    public ChannelData getLastData(String channel, boolean skipNull) {
        try {
            return thingsAdapter.getLastValue(userID, deviceID, channel, skipNull);
        } catch (IotDatabaseException ex) {
            ex.printStackTrace();
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public ChannelData getLastData(String channel) {
        try {
            return thingsAdapter.getLastValue(userID, deviceID, channel, false);
        } catch (IotDatabaseException ex) {
            ex.printStackTrace();
            return null;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public ChannelData getAverageValue(String channel, int scope) {
        return getAverageValue(channel, scope, null);
    }

    public ChannelData getAverageValue(String channel, int scope, Double newValue) {
        try {
            return thingsAdapter.getAverageValue(userID, deviceID, channel, scope, newValue);
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
            return thingsAdapter.getMinimalValue(userID, deviceID, channel, scope, newValue);
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
            return thingsAdapter.getMaximalValue(userID, deviceID, channel, scope, newValue);
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
            return thingsAdapter.getSummaryValue(userID, deviceID, channel, scope, newValue);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

}
