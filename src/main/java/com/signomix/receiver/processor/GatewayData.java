package com.signomix.receiver.processor;

public class GatewayData {
    public String gatewayId;
    public long rssi;
    public double snr;
    public int quality;

    public GatewayData() {
        // Default constructor
    }
    public GatewayData(String gatewayId, long rssi, double snr, int quality) {
        this.gatewayId = gatewayId;
        this.rssi = rssi;
        this.snr = snr;
        this.quality = quality;
    }
}
