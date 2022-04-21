/**
* Copyright (C) Grzegorz Skorupa 2018.
* Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
*/
package com.signomix.receiver.script;

import java.util.ArrayList;

import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.receiver.IotDatabaseIface;

/**
 *
 * @author greg
 */
public interface ScriptingAdapterIface {

        public ScriptResult processData1(ArrayList<ChannelData> values, Device device, long dataTimestamp,
                        Double latitude, Double longitude, Double altitude,
                        String command, String requestData, IotDatabaseIface dao) throws ScriptAdapterException;

        public ArrayList<ChannelData> decodeData(byte[] data, Device device, long dataTimestamp)
                        throws ScriptAdapterException;

        public ArrayList<ChannelData> decodeHexData(String hexPayload, Device device,
                        long dataTimestamp) throws ScriptAdapterException;

        /*
         * public ScriptResult processData(ArrayList<ChannelData> values, Device device,
         * long dataTimestamp,
         * Double latitude, Double longitude, Double altitude, Double state,
         * int alert, String command, String requestData, IotDatabaseIface dao)
         * throws ScriptAdapterException;
         */
}
