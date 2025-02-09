/**
* Copyright (C) Grzegorz Skorupa 2018.
* Distributed under the MIT License (license terms are at http://opensource.org/licenses/MIT).
*/
package com.signomix.receiver.script;

import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.Application;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import java.util.ArrayList;

/**
 *
 * @author greg
 */
public interface ScriptingAdapterIface {

        public ProcessorResult processData1(ArrayList<ChannelData> values, Device device,
                        Application application, long dataTimestamp,
                        Double latitude, Double longitude, Double altitude,
                        String command, String requestData, IotDatabaseIface dao,
                        Long port) throws ScriptAdapterException;

        public ArrayList<ChannelData> decodeData(byte[] data, Device device, Application application, long dataTimestamp)
                        throws ScriptAdapterException;

        // public ArrayList<ChannelData> decodeHexData(String hexPayload, Device device,
        // long dataTimestamp) throws ScriptAdapterException;

        /*
         * public ProcessorResult processData(ArrayList<ChannelData> values, Device
         * device,
         * long dataTimestamp,
         * Double latitude, Double longitude, Double altitude, Double state,
         * int alert, String command, String requestData, IotDatabaseIface dao)
         * throws ScriptAdapterException;
         */
}
