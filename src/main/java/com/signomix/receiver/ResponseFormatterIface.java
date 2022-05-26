package com.signomix.receiver;

import java.util.Properties;

public interface ResponseFormatterIface {
    public String format(Properties appConfig, String payload);
}
