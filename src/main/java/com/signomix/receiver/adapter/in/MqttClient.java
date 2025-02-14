package com.signomix.receiver.adapter.in;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

import com.signomix.common.api.PayloadParserIface;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.receiver.ReceiverService;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

@ApplicationScoped
public class MqttClient {

    @Inject
    Logger logger;

    @Inject
    ReceiverService service;

    @Incoming("data")
    public void processData(byte[] bytes) {
        String msg = new String(bytes);
        logger.info("Data received: " + msg);

        IotData2 iotData = parseTextData(msg, ";"); 
        Device device = service.getDevice(iotData.dev_eui);
        if (null == device) {
            logger.warn("unknown device " + iotData.dev_eui);
            return;
        }
        if (!device.isActive()) {
            return;
        }
        service.processDataNoResponse(iotData);
    }

    private IotData2 parseTextData(String input, String separator) {

        IotData2 data = new IotData2();
        HashMap<String, Object> options = new HashMap<>();
        options.put("separator", separator);
        // options.put("eui", eui);
        // options.put("euiInHeader", ""+euiHeaderFirst);
        PayloadParserIface parser = new com.signomix.receiver.PayloadParser();
        data.payload_fields = (ArrayList) parser.parse(input, options);
            data.dev_eui = getEuiParamValue(data.payload_fields);
        data.normalize();
        data.setTimestampUTC();
        data.authRequired = false;
        return data;
    }

    private String getEuiParamValue(ArrayList<Map> params) {
        Map<String, String> map;
        for (int i = 0; i < params.size(); i++) {
            map = params.get(i);
            if ("eui".equals(map.get("name"))) {
                return map.get("value");
            }
        }
        return null;
    }

}
