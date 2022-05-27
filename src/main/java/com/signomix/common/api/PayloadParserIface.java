package com.signomix.common.api;

import java.util.List;
import java.util.Map;

public interface PayloadParserIface {
    public List<Map> parse(String payload, Map options);
}
