package com.ss.rabbitmq;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class RabbitMQSpout extends BaseRichSpout {
    private Logger logger;

    private ErrorReporter reporter;

    private RabbitMQConfigurator configurator;

    private transient SpoutOutputCollector collector;

    private Map<Long, String> queueMessageMap = new HashMap<Long, String>();

    private Map<String, MessageConsumer> messageConsumers = new HashMap<String, MessageConsumer>();

    private BlockingQueue<Message> messages = new LinkedBlockingDeque<Message>();

    public RabbitMQSpout(RabbitMQConfigurator configurator, ErrorReporter reporter) {
        this(configurator, reporter, LoggerFactory.getLogger(RabbitMQSpout.class));
    }

    public RabbitMQSpout(RabbitMQConfigurator configurator, ErrorReporter reporter, Logger logger) {
        this.configurator = configurator;
        this.reporter = reporter;
        this.logger = logger;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        configurator.declareOutputFields(outputFieldsDeclarer);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;

        for (String queue : configurator.getQueueName()) {
            MessageConsumer consumer = new MessageConsumer(messages, queue,
                    spoutOutputCollector, configurator, reporter, logger);
            consumer.openConnection();
            messageConsumers.put(queue, consumer);
        }
    }

    @Override
    public void nextTuple() {
        Message message;
        try {
            while ((message = messages.take()) != null) {
                List<Object> tuple = extractTuple(message);
                if (!tuple.isEmpty()) {
                    collector.emit(tuple, message.getEnvelope().getDeliveryTag());
                    if (!configurator.isAutoAcking()) {
                        queueMessageMap.put(message.getEnvelope().getDeliveryTag(), message.getQueue());
                    }
                }
            }
        } catch (InterruptedException e) {
            logger.warn("Error in the queue ", e);
        }
    }

    @Override
    public void ack(Object msgId) {
        if (msgId instanceof Long) {
            String name =  queueMessageMap.remove(msgId);
            MessageConsumer consumer = messageConsumers.get(name);
            consumer.ackMessage((Long) msgId);
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof Long) {
            String name =  queueMessageMap.remove(msgId);
            MessageConsumer consumer = messageConsumers.get(name);
            consumer.failMessage((Long) msgId);
        }
    }

    @Override
    public void close() {
        for (MessageConsumer consumer : messageConsumers.values()) {
            consumer.closeConnection();
        }
        super.close();
    }

    public List<Object> extractTuple(Message delivery) {
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        try {
            List<Object> tuple = configurator.getMessageBuilder().deSerialize(delivery.getConsumerTag(),
                    delivery.getEnvelope(), delivery.getProperties(), delivery.getBody());
            if (tuple != null && !tuple.isEmpty()) {
                return tuple;
            }
            String errorMsg = "Deserialization error for msgId " + deliveryTag;
            logger.warn(errorMsg);
            collector.reportError(new Exception(errorMsg));
        } catch (Exception e) {
            logger.warn("Deserialization error for msgId " + deliveryTag, e);
            collector.reportError(e);
        }
        // get the malformed message out of the way by dead-lettering (if dead-lettering is configured) and move on
        MessageConsumer consumer = messageConsumers.get(delivery.getQueue());
        if (consumer != null) {
            consumer.deadLetter(deliveryTag);
        }

        return Collections.emptyList();
    }
}
