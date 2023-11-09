package com.signomix.receiver.adapter.out;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;

import io.smallrye.mutiny.Multi;

@ApplicationScoped
public class MqttSender {

/*     @Outgoing("data-received")
    public Multi<String> sendMessage(String eui) {
        return Multi.createFrom().item(eui);
    } */
    
}
