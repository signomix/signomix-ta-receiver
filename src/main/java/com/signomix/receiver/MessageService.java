package com.signomix.receiver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.MessageEnvelope;
import com.signomix.common.User;
import com.signomix.receiver.event.IotEvent;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

public class MessageService {

    private static final Logger LOG = Logger.getLogger(MessageService.class);

    @Channel("events") Emitter<IotEvent> eventsEmitter;
    //@Channel("notifications") Emitter<MessageEnvelope> iotEventEmitter;
    @Channel("notifications") Emitter<byte[]> iotEventEmitter;


    public void sendErrorInfo(IotEvent event) {
        LOG.info("sending error to MQ");
        eventsEmitter.send(event);
    }

    public void sendNotification(IotEvent event) {
        LOG.info("sending notification to MQ, origin:"+event.getOrigin());

        String[] origin = event.getOrigin().split("\t");
        User user = new User();
        user.uid = origin[0];
        
        MessageEnvelope wrapper = new MessageEnvelope();
                                wrapper.type = event.getType();
                                wrapper.eui = origin[1];
                                wrapper.message = (String) event.getPayload();
                                wrapper.user = user;

        String encodedMessage;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            encodedMessage = objectMapper.writeValueAsString(wrapper);
            iotEventEmitter.send(encodedMessage.getBytes());
        } catch (JsonProcessingException ex) {
            LOG.error(ex.getMessage());
        }
    }
    
    public void sendData(IotEvent event) {
        LOG.info("sending data to MQ");
    }
    public void sendCommand(IotEvent event) {
        LOG.info("sending command to MQ");

    }
}
