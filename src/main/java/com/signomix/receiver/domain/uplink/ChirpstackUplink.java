package com.signomix.receiver.domain.uplink;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ChirpstackUplink {

    public String deduplicationId;
    public String time;
    @JsonProperty("deviceInfo")
    public DeviceInfo deviceinfo;
    public String devAddr;
    public long dr;
    public long fPort;
    public String data;
    public List<RxInfo> rxInfo;
    @JsonProperty("txInfo")
    public TxInfo txInfo;
    public String objectJSON;

}
