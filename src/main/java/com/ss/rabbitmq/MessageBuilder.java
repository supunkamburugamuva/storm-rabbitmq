package com.ss.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import java.util.List;

public interface MessageBuilder {
    List<Object> deSerialize(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body);
}
