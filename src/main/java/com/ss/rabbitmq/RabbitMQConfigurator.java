package com.ss.rabbitmq;

import com.rabbitmq.client.ConnectionFactory;

import java.io.Serializable;

public interface RabbitMQConfigurator extends Serializable {
    ConnectionFactory getConnectionFactory();

    boolean isAutoAcking();

    int getPrefetchCount();

    boolean isReQueueOnFail();

    String getConsumerTag();

    String getQueueName();
}
