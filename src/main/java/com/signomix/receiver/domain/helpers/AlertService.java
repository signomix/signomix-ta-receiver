package com.signomix.receiver.domain.helpers;

import com.signomix.common.db.IotDatabaseException;
import com.signomix.common.db.SignalDaoIface;
import com.signomix.common.event.IotEvent;
import com.signomix.common.iot.Device;
import com.signomix.common.iot.sentinel.Signal;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

@ApplicationScoped
public class AlertService {

    @Inject
    Logger LOG;

    @Inject
    @Channel("alerts")
    Emitter<String> alertEmitter;

    void addNotifications(
        Device device,
        IotEvent event,
        String errorMessage,
        boolean withMessage,
        SignalDaoIface signalDao
    ) {
        if (null == event) {
            LOG.warn("event is null");
            return;
        }
        int alertLevel = 0;
        // INFO, ALERT and WARNING notifications are saved as signals and sentinel
        // events
        if (event.getType() == IotEvent.ALERT) {
            alertLevel = 3;
        } else if (event.getType() == IotEvent.WARNING) {
            alertLevel = 2;
        } else {
            // INFO
            alertLevel = 1;
        }

        HashSet<String> recipients = new HashSet<>();
        recipients.add(device.getUserID());
        if (device.getTeam() != null) {
            String[] r = device.getTeam().split(",");
            for (int j = 0; j < r.length; j++) {
                if (!r[j].isEmpty()) {
                    recipients.add(r[j]);
                }
            }
        }
        if (device.getAdministrators() != null) {
            String[] r = device.getAdministrators().split(",");
            for (int j = 0; j < r.length; j++) {
                if (!r[j].isEmpty()) {
                    recipients.add(r[j]);
                }
            }
        }
        IotEvent errEvent = null;
        if (null != errorMessage) {
            errEvent = new IotEvent("info", errorMessage);
        }

        Iterator itr = recipients.iterator();
        String userId;
        while (itr.hasNext()) {
            userId = (String) itr.next();
            if (null != event) {
                event.setOrigin(userId + "\t" + device.getEUI());
                // Because this kind of notification is not created by sentinel, there is no
                // sentinel event
                // associated with it and only the signal is saved
                Signal signal = new Signal();
                signal.deviceEui = device.getEUI();
                signal.level = alertLevel;
                String message = (String) event.getPayload();
                if (null == message) {
                    message = "";
                }
                if (message.length() > 255) {
                    message = message.substring(0, 250) + " ...";
                }
                signal.messageEn = message;
                signal.messagePl = message;
                signal.sentinelConfigId = -1L;
                signal.userId = userId;
                signal.createdAt = new Timestamp(event.getCreatedAt());
                signal.organizationId = device.getOrganizationId();
                try {
                    signalDao.saveSignal(signal);
                } catch (IotDatabaseException e) {
                    e.printStackTrace();
                }
                // }
                sendAlert(
                    event.getType(),
                    userId,
                    device.getEUI(),
                    (String) event.getPayload(),
                    (String) event.getPayload(),
                    event.getCreatedAt(),
                    withMessage
                );
            }
        }
        if (null != errEvent) {
            // error message is sent to device owner and administrators
            recipients.clear();
            recipients.add(device.getUserID());
            if (device.getAdministrators() != null) {
                String[] r = device.getAdministrators().split(",");
                for (int j = 0; j < r.length; j++) {
                    if (!r[j].isEmpty()) {
                        recipients.add(r[j]);
                    }
                }
            }
            itr = recipients.iterator();
            while (itr.hasNext()) {
                userId = (String) itr.next();
                sendAlert(
                    errEvent.getType(),
                    userId,
                    device.getEUI(),
                    "info",
                    errorMessage,
                    System.currentTimeMillis(),
                    withMessage
                );
            }
        }
    }

    private void sendAlert(
        String alertType,
        String userId,
        String deviceEui,
        String alertSubject,
        String alertMessage,
        long createdAt,
        boolean withMessage
    ) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending alert to userId: " + userId);
        }
        if (!withMessage) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Emitting and alert to userId: " + userId);
        }
        alertEmitter.send(
            userId +
                "\t" +
                deviceEui +
                "\t" +
                alertType +
                "\t" +
                alertMessage +
                "\t" +
                alertSubject
        );
    }
}
