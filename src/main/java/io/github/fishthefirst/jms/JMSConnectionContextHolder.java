package io.github.fishthefirst.jms;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.github.fishthefirst.utils.JMSRuntimeExceptionUtils.tryAndLogError;

@Slf4j
public final class JMSConnectionContextHolder implements AutoCloseable {
    // Final vars
    private final List<JMSContextWrapper> providedContexts = new ArrayList<>();

    // Constructor vars
    private final int sessionMode;
    private final ConnectionFactory connectionFactory;

    // JMS
    private JMSContext context;

    // User props
    private String clientId;

    JMSConnectionContextHolder(ConnectionFactory connectionFactory, int sessionMode) {
        Objects.requireNonNull(connectionFactory, "Connection factory cannot be null");
        this.connectionFactory = connectionFactory;
        this.sessionMode = sessionMode;
    }

    // User props setters
    public synchronized void setClientId(String clientId) {
        this.clientId = clientId;
        if(Objects.nonNull(context))
            onException(new JMSException("Client ID Changed", "", new Exception("Client ID Changed")));

    }

    // Context Controls
    @Override
    public synchronized void close() {


        if(Objects.nonNull(context)) {
            tryAndLogError(context::close);
        }
        context = null;
        List<JMSContextWrapper> copy = List.copyOf(providedContexts);
        providedContexts.clear();
        for (JMSContextWrapper providedContext : copy) {
            tryAndLogError(() -> providedContext.onException(new JMSException("Connection closing")));
        }
    }

    // JMS Context Methods
    int getSessionMode() {
        return sessionMode;
    }

    synchronized JMSContextWrapper createContext(int sessionMode, ExceptionListener exceptionListener) {
        if (Objects.isNull(context)) {
            buildAndAssignContext();
        }
        JMSContextWrapper jmsContextWrapper = new JMSContextWrapper(context.createContext(sessionMode), exceptionListener);
        providedContexts.add(jmsContextWrapper);
        return jmsContextWrapper;
    }

    private void buildAndAssignContext() {
        try {
            // This protects connection factory methods from blowing up from multiple connection holders
            synchronized (connectionFactory) {
                context = connectionFactory.createContext(sessionMode);
            }
            context.setClientID(clientId);
            context.setExceptionListener(this::onException);
            if(Objects.nonNull(clientId)) {
                log.info("Connection Context built with client ID {}", clientId);
            }
            else {
                log.warn("Connection Context built without client ID");
            }
        }
        catch (Exception e) {
            context = null;
            log.error("Failed to build Main Context");
            throw e;
        }
    }

    private synchronized void onException(JMSException exception) {
        log.error("Connection Context expired: {}", exception.getMessage());
        List<JMSContextWrapper> copyOfWrappers = List.copyOf(providedContexts);
        close();
        copyOfWrappers.forEach(jmsContextWrapper -> jmsContextWrapper.onException(exception));
    }
}