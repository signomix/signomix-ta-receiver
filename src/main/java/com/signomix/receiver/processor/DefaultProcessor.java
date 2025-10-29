package com.signomix.receiver.processor;

import java.util.ArrayList;
import java.util.HashMap;

import org.jboss.logging.Logger;

import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.iot.Application;
import com.signomix.common.iot.ApplicationConfig;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.chirpstack.uplink.ChirpstackUplink;
import com.signomix.common.iot.chirpstack.uplink.RxInfo;
import com.signomix.common.iot.ttn3.TtnData3;

public class DefaultProcessor implements DataProcessorIface {

    private static final Logger logger = Logger.getLogger(DefaultProcessor.class);

    // @Inject
    // @Channel("error-event")
    // Emitter<String> errorEventEmitter;

    @Override
    public ProcessorResult getProcessingResult(
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
            TtnData3 ttnUplink) throws Exception {
        // Default implementation can be empty or throw an exception

        int actualParameters = listOfValues.size();
        long spreadingFactor;
        ProcessorResult result = new ProcessorResult();
        logger.debug("start processing");
        for (ChannelData channelData : listOfValues) {
            if (logger.isDebugEnabled()) {
                logger.debug("channel data: " + channelData.getName() +
                        ", value: " + channelData.getValue() +
                        ", timestamp: " + channelData.getTimestamp());
            }
            result.putData(
                    channelData.getDeviceEUI(),
                    channelData.getName(),
                    channelData.getValue(),
                    channelData.getTimestamp(),
                    channelData.getStringValue());
        }
        result.setDeviceStatus(device.getState());
        if (chirpstackUplink == null && ttnUplink == null) {
            return result;
        }

        ApplicationConfig appConfig = application!=null?application.config:null;
        HashMap<String, Object> devConfig = device.getConfigurationMap();
        boolean getTransmissionData = false;
        if (devConfig.containsKey("processor.getTransmissionData")) {
            getTransmissionData = (Boolean) devConfig.get("processor.getTransmissionData");
        } else if (appConfig != null && appConfig.containsKey("processor.getTransmissionData")) {
            getTransmissionData = Boolean.parseBoolean(appConfig.get("processor.getTransmissionData"));
        }
        if (!getTransmissionData) {
            // If the application or device configuration does not require transmission
            // data,
            // return the result without further processing.
            return result;
        }

        // Calculate the number of gateways to store based on the actual parameters
        // and the ChirpStack or TTN uplink data.
        // The formula is: (24 - actualParameters) / 4
        int numberOfGatewaysToStore = (24 - actualParameters) / 4;
        if (chirpstackUplink != null) {
            long dataRate = chirpstackUplink.dr;
            try {
                spreadingFactor = chirpstackUplink.txInfo.modulation.get("lora").spreadingFactor;
            } catch (Exception e) {
                spreadingFactor = 0; // Default value if not available
            }
            result.putData(
                    device.getEUI(),
                    "dr",
                    dataRate,
                    dataTimestamp,
                    String.valueOf(dataRate));
            result.putData(
                    device.getEUI(),
                    "sf",
                    spreadingFactor,
                    dataTimestamp,
                    String.valueOf(spreadingFactor));

            RxInfo rxInfo;
            final long sf = spreadingFactor;
            ArrayList<GatewayData> gateways = new ArrayList<>();
            chirpstackUplink.rxInfo.forEach(rxi -> {
                rxi.gatewayId = rxi.gatewayId;
                long rssi = rxi.rssi;
                double snr = rxi.snr;
                int quality = LoRaWanAnalyzer.analyze(sf, snr, rssi).quality();
                GatewayData gatewayData = new GatewayData(rxi.gatewayId, rssi, snr, quality);
                gateways.add(gatewayData);
            });
            // sort gateways by quality - highest first
            gateways.sort((g1, g2) -> Integer.compare(g2.quality, g1.quality));
            // number of gateways to store is 24 - actual parameters divided by 4
            long gatewayId;
            for (int i = 0; i < Math.min(numberOfGatewaysToStore, gateways.size()); i++) {
                GatewayData gateway = gateways.get(i);
                try{
                    //decode gateway ID from hex string
                    gatewayId = Long.parseLong(gateway.gatewayId, 16);
                }catch (NumberFormatException e) {
                    logger.error("Invalid gateway ID format: " + gateway.gatewayId, e);
                    gatewayId = 0; // Default value if parsing fails
                }
                result.putData(
                        device.getEUI(),
                        "gid_" + (i),
                        gatewayId,
                        dataTimestamp,
                        gateway.gatewayId);
                result.putData(
                        device.getEUI(),
                        "rssi_" + (i),
                        gateway.rssi,
                        dataTimestamp,
                        String.valueOf(gateway.rssi));
                result.putData(
                        device.getEUI(),
                        "snr_" + (i),
                        gateway.snr,
                        dataTimestamp,
                        String.valueOf(gateway.snr));
                result.putData(
                        device.getEUI(),
                        "quality_" + (i),
                        gateway.quality,
                        dataTimestamp,
                        String.valueOf(gateway.quality));
            }
        } else if (ttnUplink != null) {
            // TTN 3.x data processing
            long dataRate = 0; // TODO: ttnUplink.dataRate;
            spreadingFactor = 0; // TODO
            result.putData(
                    device.getEUI(),
                    "dr",
                    dataRate,
                    dataTimestamp,
                    String.valueOf(dataRate));
            result.putData(
                    device.getEUI(),
                    "sf",
                    spreadingFactor,
                    dataTimestamp,
                    String.valueOf(spreadingFactor));
            ArrayList<GatewayData> gateways = new ArrayList<>();
            // TODO: gateways
            /*
             * ttnUplink.rxInfo.forEach(rxi -> {
             * rxi.gatewayId = rxi.gatewayId;
             * long rssi = rxi.rssi;
             * double snr = rxi.snr;
             * int quality = LoRaWanAnalyzer.analyze(spreadingFactor, snr, rssi).quality();
             * GatewayData gatewayData = new GatewayData(rxi.gatewayId, rssi, snr, quality);
             * gateways.add(gatewayData);
             * });
             */
            // sort gateways by quality - highest first
            gateways.sort((g1, g2) -> Integer.compare(g2.quality, g1.quality));
            // add first 5 gateways to result
            for (int i = 0; i < Math.min(numberOfGatewaysToStore, gateways.size()); i++) {
                GatewayData gateway = gateways.get(i);
                result.putData(
                        device.getEUI(),
                        "gid_" + (i),
                        gateway.gatewayId,
                        dataTimestamp,
                        gateway.gatewayId);
                result.putData(
                        device.getEUI(),
                        "rssi_" + (i),
                        gateway.rssi,
                        dataTimestamp,
                        String.valueOf(gateway.rssi));
                result.putData(
                        device.getEUI(),
                        "snr_" + (i),
                        gateway.snr,
                        dataTimestamp,
                        String.valueOf(gateway.snr));
                result.putData(
                        device.getEUI(),
                        "quality_" + (i),
                        gateway.quality,
                        dataTimestamp,
                        String.valueOf(gateway.quality));
            }
        }

        return result;
    }

    @Override
    public ProcessorResult getProcessingResult(
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
            Long port) throws Exception {
        // Default implementation can be empty or throw an exception
        return new ProcessorResult();
    }

}
