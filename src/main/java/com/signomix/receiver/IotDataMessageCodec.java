package com.signomix.receiver;

import java.util.HashMap;
import java.util.Map;

import com.signomix.common.iot.generic.IotData2;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Custeom codec
 * 
 * https://github.com/vert-x3/vertx-examples/blob/4.x/core-examples/src/main/java/io/vertx/example/core/eventbus/messagecodec/util/CustomMessageCodec.java
 */
public class IotDataMessageCodec implements MessageCodec<IotData2, IotData2> {
    @Override
    public void encodeToWire(Buffer buffer, IotData2 iotData2) {
        JsonArray fields = new JsonArray();
        JsonObject channelField;
        Map field;
        for (int i = 0; i < iotData2.payload_fields.size(); i++) {
            field = iotData2.payload_fields.get(i);
            channelField = new JsonObject();
            channelField.put("name", field.get("name"));
            channelField.put("value", "" + field.get("value"));
            channelField.put("stringValue", "" + field.get("value"));
            fields.add(channelField);
        }

        // Easiest ways is using JSON object
        JsonObject jsonToEncode = new JsonObject();
        jsonToEncode.put("eui", iotData2.getDeviceEUI());
        jsonToEncode.put("timestamp", iotData2.timestamp);
        jsonToEncode.put("time", iotData2.time);
        jsonToEncode.put("gateway", iotData2.gateway_eui);
        jsonToEncode.put("authrequired", iotData2.authRequired);
        jsonToEncode.put("fields", fields);

        // Encode object to string
        String jsonToStr = jsonToEncode.encode();

        // Length of JSON: is NOT characters count
        int length = jsonToStr.getBytes().length;

        // Write data into given buffer
        buffer.appendInt(length);
        buffer.appendString(jsonToStr);
    }

    @Override
    public IotData2 decodeFromWire(int position, Buffer buffer) {
        // My custom message starting from this *position* of buffer
        int _pos = position;
        long systemTimestamp = System.currentTimeMillis();
        // Length of JSON
        int length = buffer.getInt(_pos);

        // Get JSON string by it`s length
        // Jump 4 because getInt() == 4 bytes
        String jsonStr = buffer.getString(_pos += 4, _pos += length);
        JsonObject contentJson = new JsonObject(jsonStr);
        JsonArray fields;

        IotData2 data = new IotData2(systemTimestamp);
        data.dev_eui = contentJson.getString("eui");
        data.timestamp = contentJson.getString("timestamp");
        data.time = contentJson.getString("time");
        data.gateway_eui = contentJson.getString("gateway");
        data.authRequired = contentJson.getBoolean("authrequired");
        fields = contentJson.getJsonArray("fields");


        // Get fields
        JsonObject field;
        HashMap<String, String> map;
        for (int i = 0; i < fields.size(); i++) {
            field = fields.getJsonObject(i);
            map = new HashMap<>();
            map.put("name", field.getString("name"));
            map.put("value", field.getString("value"));
            map.put("stringValue", field.getString("stringValue"));
            data.payload_fields.add(map);
        }
        // We can finally create custom message object
        data.setTimestampUTC(systemTimestamp);
        data.prepareIotValues(systemTimestamp);
        return data;
    }

    @Override
    public IotData2 transform(IotData2 iotData2) {
        // If a message is sent *locally* across the event bus.
        // This example sends message just as is
        return iotData2;
    }

    @Override
    public String name() {
        // Each codec must have a unique name.
        // This is used to identify a codec when sending a message and for unregistering
        // codecs.
        return this.getClass().getSimpleName();
    }

    @Override
    public byte systemCodecID() {
        // Always -1
        return -1;
    }
}
