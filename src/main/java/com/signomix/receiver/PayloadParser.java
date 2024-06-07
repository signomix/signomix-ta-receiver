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

    public PayloadParser() {
    }

    @Override
    public List<Map> parse(String payload, Map options) {
        String separator = (String) options.get("separator");
        return parseLines(payload, separator);
    }

    private List<Map> parseLines(String payload, String separator) {
        ArrayList payload_fields = new ArrayList<>();
        String dataSeparator = separator!=null?separator:";";  
        Scanner scanner = new Scanner(payload);
        HashMap<String, String> map;
        String line;
        String[] data;
        String[] dataField;

        while (scanner.hasNextLine()) {
            line = scanner.nextLine();
            LOG.debug("line:" + line);
            data = line.split(dataSeparator);
            for (String field : data) {
                dataField = field.split("=");
                if (dataField.length == 2) {
                    map = new HashMap<>();
                    map.put("name", dataField[0].trim());
                    map.put("value", dataField[1].trim());
                    payload_fields.add(map);
                }
            }
        }
        return payload_fields;
    }

}
