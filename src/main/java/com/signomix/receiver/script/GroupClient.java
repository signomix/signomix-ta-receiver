/**
 * Copyright (C) Grzegorz Skorupa 2025.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.receiver.script;

import java.util.ArrayList;
import java.util.List;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.Device;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
public class GroupClient {

    IotDatabaseIface thingsAdapter;
    String userID;
    String deviceGroups;
    ArrayList<String> groups;

    public GroupClient(String userID, String deviceGroups, IotDatabaseIface thingsAdapter) {
        this.thingsAdapter = thingsAdapter;
        this.userID = userID;
        this.deviceGroups = deviceGroups;
        groups = new ArrayList<>();
        String[] parts = deviceGroups.split(",");
        for (String part : parts) {
            if (!part.isEmpty()) {
                groups.add(part);
            }
        }
    }

    public String[] getGroupDevices(String groupEui, String euiToSkip) {
        if(!groups.contains(groupEui)){
            return new String[0];
        }
        try {
            List<Device> devices = thingsAdapter.getGroupDevices(groupEui);
            List<String> resultList = new ArrayList<>();
            for (int i = 0; i < devices.size(); i++) {
                if(null == euiToSkip || !devices.get(i).getEUI().equals(euiToSkip)){
                    resultList.add(devices.get(i).getEUI());
                }
            }
            return resultList.toArray(new String[0]);
        } catch (IotDatabaseException ex) {
            ex.printStackTrace();
            return new String[0];
        } catch (Exception ex) {
            ex.printStackTrace();
            return new String[0];
        }
    }

    public String[] getGroupVirtualDevices(String groupEui, String euiToSkip) {
        if(!groups.contains(groupEui)){
            return new String[0];
        }
        try {
            List<Device> devices = thingsAdapter.getGroupVirtualDevices(groupEui);
            List<String> resultList = new ArrayList<>();
            for (int i = 0; i < devices.size(); i++) {
                if(null == euiToSkip || !devices.get(i).getEUI().equals(euiToSkip)){
                    resultList.add(devices.get(i).getEUI());
                }
            }
            return resultList.toArray(new String[0]);
        } catch (IotDatabaseException ex) {
            ex.printStackTrace();
            return new String[0];
        } catch (Exception ex) {
            ex.printStackTrace();
            return new String[0];
        }
    }

}
