package com.signomix.common.api;

import java.util.HashMap;

public interface ResponseTransformerIface {
    public String transform(HashMap<String,Object> appConfig, String payload);
}
