package io.github.fishthefirst.jms;

import io.github.fishthefirst.contextproviders.JMSContextProvider;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public class JMSMainContextHolder implements JMSContextProvider, ExceptionListener {
    private JMSContext context;
    private final int sessionMode;
    private final ConnectionFactory connectionFactory;

    public JMSMainContextHolder(ConnectionFactory connectionFactory, int sessionMode) {
        Objects.requireNonNull(connectionFactory, "Connection factory cannot be null");
        this.connectionFactory = connectionFactory;
        this.sessionMode = sessionMode;
    }

    public JMSContext createContext() {
        return createContext(sessionMode);
    }

    public JMSContext createContext(int sessionMode) {
        if (Objects.isNull(context)) {
            buildAndAssignContext();
        }
        return context.createContext(sessionMode);
    }

    private synchronized void buildAndAssignContext() {
        try {
            context = connectionFactory.createContext(sessionMode);
            context.setExceptionListener(this);
        } catch (Exception e) {
            context = null;
            throw e;
        }
    }

    @Override
    public void onException(JMSException exception) {
        log.error("", exception);
        try {
            buildAndAssignContext();
        } catch (Exception e2) {
            log.error("", e2);
        }
    }
}