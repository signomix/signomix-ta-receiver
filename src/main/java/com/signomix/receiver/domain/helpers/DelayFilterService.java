package com.signomix.receiver.domain.helpers;

import com.signomix.common.iot.ttn3.TtnData3;
import com.signomix.common.iot.tts.RxMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import org.jboss.logging.Logger;

@ApplicationScoped
public class DelayFilterService {

    @Inject
    Logger LOG;

    public boolean isDelayAccepted(
        TtnData3 dataObject,
        long maxDelay,
        long delayLimit
    ) {
        if (dataObject == null || dataObject.rxMetadata == null) {
            return false;
        }
        boolean delayed = false;
        long receivedTimestamp = dataObject.receivedAt;
        long start = Instant.parse("2020-01-01T00:00:00Z").toEpochMilli();

        long upperBound = receivedTimestamp + delayLimit;
        Long maxTimestamp = null;

        for (RxMetadata metadata : dataObject.rxMetadata) {
            if (metadata == null || metadata.getTime() == null) {
                continue;
            }
            long ta = metadata.getTime().getTime();
            if (
                ta > start &&
                ta <= upperBound &&
                (maxTimestamp == null || ta > maxTimestamp)
            ) {
                maxTimestamp = ta;
            }
        }
        if (maxTimestamp == null) {
            // for simulated uplinks
            return true;
        }
        delayed = receivedTimestamp - maxTimestamp > maxDelay;
        if (delayed) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    dataObject.deviceEui +
                        " data is too delayed, receivedAt: " +
                        dataObject.receivedAt +
                        ", maxTimestamp: " +
                        maxTimestamp
                );
            }
            return false;
        } else {
            return true;
        }
    }
}
