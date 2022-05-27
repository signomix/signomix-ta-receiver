package com.signomix.receiver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.signomix.common.api.PayloadParserIface;

import org.jboss.logging.Logger;

public class PayloadParser implements PayloadParserIface {
    private static final Logger LOG = Logger.getLogger(PayloadParser.class);
    public PayloadParser(){
    }

    @Override
    public List<Map> parse(String payload, Map options){
        ArrayList payload_fields=new ArrayList<>();
        Scanner scanner = new Scanner(payload);
        HashMap<String, String> map;
        int idx;
        String name;
        String value;
        String line;
        while (scanner.hasNextLine()) {
            line = scanner.nextLine();
            LOG.debug("line:"+line);
            idx = line.indexOf("=");
            if (idx > 0 && idx < line.length()) {
                name = line.substring(0, idx);
                value = line.substring(idx + 1);
                map = new HashMap<>();
                map.put("name",name.trim());
                map.put("value", value.trim());
                payload_fields.add(map);
            }
        }
        return payload_fields;
    }
    
}
