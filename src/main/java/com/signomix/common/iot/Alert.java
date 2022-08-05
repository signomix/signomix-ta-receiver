/**
* Copyright (C) Grzegorz Skorupa 2018.
* Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
*/
package com.signomix.common.iot;

import com.signomix.common.event.IotEvent;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
public class Alert extends IotEvent {

    private String userID = null;
    private String deviceEUI = null;

    public Alert() {
        super();
    }

    public Alert(IotEvent event) {
        super();
        setCategory(event.getCategory());
        setCreatedAt(event.getCreatedAt());
        setOrigin(event.getOrigin());
        setPayload(event.getPayload());
        setType(event.getType());
    }

    @Override
    public void setOrigin(String origin) {
        super.setOrigin(origin);
        int pos = getOrigin().indexOf("\t");
        if (pos > -1) {
            userID = getOrigin().substring(0, pos);
            deviceEUI = getOrigin().substring(pos + 1);
        }
    }

    public String getUserID() {
        return userID;
    }

    public String getDeviceEUI() {
        return deviceEUI;
    }

}
