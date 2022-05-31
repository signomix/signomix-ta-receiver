package com.signomix.common.api;

import java.util.Map;

public interface ResponseTransformerIface {
    public String transform(String payload, Map<String,Object> options);
}
