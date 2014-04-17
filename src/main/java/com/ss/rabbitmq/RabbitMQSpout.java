package com.ss.rabbitmq;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.rabbitmq.client.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;

public class RabbitMQSpout extends BaseRichSpout {
    private int prefetchCount;
    private String queueName;
    private boolean requeueOnFail;
    private Logger logger;

    private Connection connection;
    private Channel channel;
    private QueueingConsumer consumer;
    private String consumerTag;

    private ErrorReporter reporter;

    private RabbitMQConfigurator configurator;

    public enum State {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

    }

    @Override
    public void nextTuple() {

    }

    private void reset() {
        consumerTag = null;
    }

    private void reinitIfNecessary() {
        if (consumerTag == null || consumer == null) {
            close();
            open();
        }
    }

    public void open() {
        try {
            connection = createConnection();
            channel = connection.createChannel();
            if (prefetchCount > 0) {
                logger.info("setting basic.qos / prefetch count to " + prefetchCount + " for " + queueName);
                channel.basicQos(prefetchCount);
            }

            consumer = new QueueingConsumer(channel);
            consumerTag = channel.basicConsume(queueName, configurator.isAutoAcking(), consumer);
        } catch (Exception e) {
            reset();
            logger.error("could not open listener on queue " + queueName);
            reporter.reportError(e);
        }
    }

    private Connection createConnection() throws IOException {
        Connection connection = configurator.getConnectionFactory().newConnection(Executors.newScheduledThreadPool(10));
        connection.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {
                logger.error("shutdown signal received", cause);
                reporter.reportError(cause);
                reset();
            }
        });
        logger.info("connected to rabbitmq: " + connection + " for " + queueName);
        return connection;
    }
}
