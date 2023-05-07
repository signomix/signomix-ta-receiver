package com.signomix.receiver;

import javax.enterprise.event.Observes;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.signomix.common.EventEnvelope;
import com.signomix.common.MessageEnvelope;
import com.signomix.common.User;
import com.signomix.common.db.IotDatabaseDao;
import com.signomix.common.db.IotDatabaseIface;
import com.signomix.common.event.IotEvent;
import com.signomix.common.event.MessageServiceIface;

import io.agroal.api.AgroalDataSource;
import io.quarkus.runtime.StartupEvent;

public class MessageService implements MessageServiceIface {

    private static final Logger LOG = Logger.getLogger(MessageService.class);

    @Channel("events")
    //Emitter<IotEvent> eventsEmitter;
    Emitter<byte[]> eventEmitter;

    @Channel("notifications")
    Emitter<byte[]> iotEventEmitter;

    @Inject
    AgroalDataSource ds;

    IotDatabaseIface dao;

    public void onApplicationStart(@Observes StartupEvent event) {
        dao = new IotDatabaseDao();
        dao.setDatasource(ds);
    }

    /*public void sendErrorInfo(IotEvent event) {
        EventEnvelope wrapper=new EventEnvelope();
        wrapper.type=EventEnvelope.ERROR;
        LOG.info("sending error to MQ");
        eventsEmitter.send(event);
    }*/
    @Override
    public void sendEvent(EventEnvelope wrapper) {
        LOG.info("sending event to MQ");
        String encodedMessage;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            encodedMessage = objectMapper.writeValueAsString(wrapper);
            eventEmitter.send(encodedMessage.getBytes());
        } catch (JsonProcessingException ex) {
            LOG.error(ex.getMessage());
        }
    }

    @Override
    public void sendNotification(IotEvent event) {
        LOG.info("sending notification to MQ, origin:" + event.getOrigin());

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

    @Override
    public void sendData(IotEvent event) {
        LOG.info("sending data to MQ");
    }

    @Override
    public void sendCommand(IotEvent event) {
        LOG.info("sending command to MQ");
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
            encodedMessage = objectMapper.writeValueAsString(event);
            eventEmitter.send(encodedMessage.getBytes());
        } catch (JsonProcessingException ex) {
            LOG.error(ex.getMessage());
        }
    }
}
