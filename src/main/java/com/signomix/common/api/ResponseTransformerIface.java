package com.signomix.common.api;

import java.util.Map;

public interface ResponseTransformerIface {
    public String transform(String payload, Map<String,Object> options);
    public Map<String,String> getHeaders(Map<String,Object> options, String input, String response);
}
