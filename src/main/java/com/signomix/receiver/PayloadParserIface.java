package com.signomix.receiver;

import java.util.List;
import java.util.Map;

public interface PayloadParserIface {
    public List<Map> parse(String payload, Map options);
}
