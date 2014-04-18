package com.ss.rabbitmq;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class RabbitMQSpout extends BaseRichSpout {
    private Logger logger;

    private Connection connection;

    private Channel channel;

    private QueueingConsumer consumer;

    private String consumerTag;

    private ErrorReporter reporter;

    private RabbitMQConfigurator configurator;

    private transient SpoutOutputCollector collector;

    private State state = State.INIT;

    public RabbitMQSpout(RabbitMQConfigurator configurator, ErrorReporter reporter) {
        this(configurator, reporter, LoggerFactory.getLogger(RabbitMQSpout.class));
    }

    public RabbitMQSpout(RabbitMQConfigurator configurator, ErrorReporter reporter, Logger logger) {
        this.configurator = configurator;
        this.reporter = reporter;
        this.logger = logger;
    }

    public enum State {
        INIT,
        CONNECTED,
        ERROR,
        RESTARTING
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void open(Map map, TopologyContext topologyContext, final SpoutOutputCollector spoutOutputCollector) {
        ErrorReporter reporter = new ErrorReporter() {
            @Override
            public void reportError(Throwable error) {
                spoutOutputCollector.reportError(error);
            }
        };

        collector = spoutOutputCollector;

        if (state == State.INIT) {
            openConnection();
        }
    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void ack(Object msgId) {
        if (msgId instanceof Long) {
            ackMessage((Long) msgId);
        }
    }

    @Override
    public void fail(Object msgId) {
        if (msgId instanceof Long) {
            failMessage((Long) msgId);
        }
    }

    @Override
    public void close() {
        closeConnection();
        super.close();
    }

    private void reset() {
        consumerTag = null;
    }

    private void reinitIfNecessary() {
        if (consumerTag == null || consumer == null) {
            closeConnection();
            openConnection();
        }
    }

    public void closeConnection() {
        try {
            if (channel != null && channel.isOpen()) {
                if (consumerTag != null) channel.basicCancel(consumerTag);
                channel.close();
            }
        } catch (Exception e) {
            logger.debug("error closing channel and/or cancelling consumer", e);
        }
        try {
            logger.info("closing connection to rabbitmq: " + connection);
            connection.close();
        } catch (Exception e) {
            logger.debug("error closing connection", e);
        }
        consumer = null;
        consumerTag = null;
        channel = null;
        connection = null;
    }

    public void openConnection() {
        try {
            connection = createConnection();
            channel = connection.createChannel();

            if (configurator.getPrefetchCount() > 0) {
                logger.info("setting basic.qos / prefetch count to " + configurator.getPrefetchCount() + " for " + configurator.getQueueName());
                channel.basicQos(configurator.getPrefetchCount());
            }

            consumer = new QueueingConsumer(channel);
            consumerTag = channel.basicConsume(configurator.getQueueName(), configurator.isAutoAcking(), consumer);
        } catch (Exception e) {
            reset();
            logger.error("could not open listener on queue " + configurator.getQueueName());
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
        logger.info("connected to rabbitmq: " + connection + " for " + configurator.getQueueName());
        return connection;
    }

    public void ackMessage(Long msgId) {
        try {
            channel.basicAck(msgId, false);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to ack message", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("could not ack for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    public void failMessage(Long msgId) {
        if (requeueOnFail)
            failWithRedelivery(msgId);
        else
            deadLetter(msgId);
    }

    public void failWithRedelivery(Long msgId) {
        try {
            channel.basicReject(msgId, true);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to fail with redelivery", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("could not fail with redelivery for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    public void deadLetter(Long msgId) {
        try {
            channel.basicReject(msgId, false);
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to fail with no redelivery", sse);
            reporter.reportError(sse);
        } catch (Exception e) {
            logger.error("could not fail with dead-lettering (when configured) for msgId: " + msgId, e);
            reporter.reportError(e);
        }
    }

    private List<Object> extractTuple(Envelope envelope, byte []body, ) {
        long deliveryTag = envelope.getDeliveryTag();
        try {
            List<Object> tuple = scheme.deserialize(message);
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
        deadLetter(deliveryTag);
        return Collections.emptyList();
    }

    public QueueingConsumer.Delivery nextMessage() {
        reinitIfNecessary();        
        try {
            return consumer.nextDelivery();
        } catch (ShutdownSignalException sse) {
            reset();
            logger.error("shutdown signal received while attempting to get next message", sse);
            reporter.reportError(sse);
            return Message.NONE;
        } catch (InterruptedException ie) {
            /* nothing to do. timed out waiting for message */
            logger.debug("interruepted while waiting for message", ie);
            return Message.NONE;
        } catch (ConsumerCancelledException cce) {
            /* if the queue on the broker was deleted or node in the cluster containing the queue failed */
            reset();
            logger.error("consumer got cancelled while attempting to get next message", cce);
            reporter.reportError(cce);
            return Message.NONE;
        }
    }
}
