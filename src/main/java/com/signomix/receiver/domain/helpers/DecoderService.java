package com.signomix.receiver.domain.helpers;

import com.signomix.common.iot.Application;
import com.signomix.common.iot.ChannelData;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.generic.IotData2;
import com.signomix.receiver.script.NashornScriptingAdapter;
import com.signomix.receiver.script.ScriptAdapterException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import org.jboss.logging.Logger;

@ApplicationScoped
class DecoderService {

    @Inject
    Logger LOG;

    @Inject
    NashornScriptingAdapter scriptingAdapter;

    ArrayList<ChannelData> decodePayload(
        IotData2 data,
        Device device,
        Application application
    ) throws ScriptAdapterException {
        if (null == device) {
            LOG.warn("device is null");
            return new ArrayList<>();
        }
        ArrayList<ChannelData> values = new ArrayList<>();
        byte[] emptyBytes = {};
        byte[] byteArray = null;
        String deviceDecoderScript = device.getEncoderUnescaped();
        if (
            (null == deviceDecoderScript ||
                deviceDecoderScript.trim().isEmpty()) &&
            null != application
        ) {
            deviceDecoderScript = application.decoder;
        }
        if (null != deviceDecoderScript && deviceDecoderScript.length() > 0) {
            if (null != data.getPayload()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("base64Payload: " + data.getPayload());
                }
                Decoder base64Decoder = Base64.getDecoder();
                if (null == base64Decoder) {
                    LOG.warn("decoder is null");
                    return values;
                }
                byteArray = base64Decoder.decode(data.getPayload().getBytes());
            } else if (null != data.getHexPayload()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                        device.getEUI() + " hexPayload: " + data.getHexPayload()
                    );
                }
                byteArray = getByteArray(data.getHexPayload());
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(device.getEUI() + " payload is null");
                }
                byteArray = emptyBytes;
            }
            if (null == byteArray) {
                byteArray = emptyBytes;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    device.getEUI() +
                        " byteArray: " +
                        Arrays.toString(byteArray)
                );
            }
            try {
                values = scriptingAdapter.decodeData(
                    byteArray,
                    device.getEUI(),
                    deviceDecoderScript,
                    data.getTimestamp()
                );
                // } catch (ScriptAdapterException ex) {
                //     ex.printStackTrace();
                //     addNotifications(device, null, ex.getMessage(), false);
                //     values = new ArrayList<>();
            } catch (Exception e) {
                // e.printStackTrace();
                // addNotifications(device, null, e.getMessage(), false);
                // values = new ArrayList<>();
                throw new ScriptAdapterException(1000, e.getMessage());
            }
        }
        if (!data.getDataList().isEmpty()) {
            for (int i = 0; i < data.getDataList().size(); i++) {
                values.add(data.getDataList().get(i));
            }
        }
        return values;
    }

    private byte[] getByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) +
                Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
}
