package com.ss.rabbitmq;

public interface ErrorReporter {
    void reportError(Throwable t);
}
