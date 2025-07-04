package com.signomix.receiver.processor;

import java.util.ArrayList;

import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.Application;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.chirpstack.uplink.ChirpstackUplink;
import com.signomix.common.iot.ttn3.TtnData3;

public interface DataProcessorIface {
    ProcessorResult getProcessingResult(
        ArrayList<ChannelData> listOfValues,
        Device device,
        Application application,
        long dataTimestamp,
        Double latitude,
        Double longitude,
        Double altitude,
        String requestData,
        String command,
        IotDatabaseIface dao,
        Long port
    ) throws Exception;

    ProcessorResult getProcessingResult(
        ArrayList<ChannelData> listOfValues,
        Device device,
        Application application,
        long dataTimestamp,
        Double latitude,
        Double longitude,
        Double altitude,
        String requestData,
        String command,
        IotDatabaseIface dao,
        Long port,
        ChirpstackUplink chirpstackUplink,
        TtnData3 ttnUplink
    ) throws Exception;


}