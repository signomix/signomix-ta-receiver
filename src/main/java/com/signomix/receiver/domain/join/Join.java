package com.signomix.receiver.domain.join;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Join {

    public String deduplicationId;
    public String time;
    @JsonProperty("deviceInfo")
    public DeviceInfo deviceinfo;
    public String devAddr;

}
