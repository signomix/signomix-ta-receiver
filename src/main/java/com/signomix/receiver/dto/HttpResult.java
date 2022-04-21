package com.signomix.receiver.dto;

import java.util.HashMap;

public class HttpResult {
    public int code;
    public HashMap<String,String> headers;
    public String payload;

    public HttpResult(){
        headers=new HashMap<>();
        code=200;
        payload="";
    }
}
