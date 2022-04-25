/**
 * Copyright (C) Grzegorz Skorupa 2018.
 * Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
 */
package com.signomix.common.iot.generic;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.signomix.common.DateTool;
import com.signomix.common.IotDataIface;
import com.signomix.common.iot.ChannelData;

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
