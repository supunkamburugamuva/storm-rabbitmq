package com.ss.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;

import java.io.Serializable;

public interface RabbitMQConfigurator extends Serializable {
    ConnectionFactory getConnectionFactory();

    boolean isAutoAcking();
}
