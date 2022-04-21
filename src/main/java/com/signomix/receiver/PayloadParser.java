package com.signomix.receiver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class PayloadParser implements PayloadParserIface {
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
            idx = line.indexOf("=");
            if (idx > 0 && idx < line.length()) {
                name = line.substring(0, idx);
                value = line.substring(idx + 1);
                map = new HashMap<>();
                map.put(name.trim(), value.trim());
                payload_fields.add(map);
            }
        }
        return payload_fields;
    }
    
}
