package com.ss.rabbitmq;


import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

import java.util.List;

public interface MessageBuilder {
    List<Object> deSerialize(QueueingConsumer.Delivery delivery);
}
