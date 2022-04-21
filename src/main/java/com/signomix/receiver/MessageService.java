package com.signomix.receiver;

import com.signomix.receiver.event.IotEvent;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import org.jboss.logging.Logger;

public class MessageService {

    private static final Logger LOG = Logger.getLogger(MessageService.class);

    @Channel("events") Emitter<IotEvent> quoteRequestEmitter;

    public void sendErrorInfo(IotEvent event) {
        LOG.info("sending to MQ");
        quoteRequestEmitter.send(event);
    }
}
