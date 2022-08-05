/**
 * Copyright (C) Grzegorz Skorupa 2018.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.common.iot.generic;

import java.util.ArrayList;
import java.util.Map;

/**
 *
 * @author Grzegorz Skorupa <g.skorupa at gmail.com>
 */
public class IotDto {

    public String dev_eui;
    public String gateway_eui;
    public ArrayList<Map> payload_fields;
    public boolean authRequired = false;
    public long timestamp;
    public String clientname;

    public IotDto() {
    }
}
