package io.github.fishthefirst.jmscontextprovider.jms;

import io.github.fishthefirst.jmscontextprovider.exceptions.ExceptionPointer;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static io.github.fishthefirst.jmscontextprovider.utils.JMSRuntimeExceptionUtils.tryAndLogError;

public class JMSConnectionContextHolder implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(JMSConnectionContextHolder.class);

    // Constructor vars
    private final ConnectionFactory connectionFactory;
    private final ExceptionPointer exceptionPointer = new ExceptionPointer(60000);

    // Object vars
    private List<JMSContextWrapper> providedContexts = new ArrayList<>();

    // JMS
    private JMSContext context;

    // User props
    private String clientId;
    private boolean allowContextWithoutClientId = true;

    JMSConnectionContextHolder(ConnectionFactory connectionFactory) {
        Objects.requireNonNull(connectionFactory, "Connection factory cannot be null");
        this.connectionFactory = connectionFactory;
    }

    // User props setters
    public synchronized void setClientId(String clientId) {
        if(Objects.equals(this.clientId, clientId)) {
            if (Objects.nonNull(context)) {
                log.warn("Tried setting client ID to the already set value: \"{}\". Ignoring.", clientId);
            }
            return;
        }
        if(!allowContextWithoutClientId && (Objects.isNull(clientId) || clientId.isBlank())) {
            throw new IllegalArgumentException("Cannot set null/blank client ID: allowContextWithoutClientId is false.");
        }
        this.clientId = clientId;
        if (Objects.nonNull(context)) {
            onException(new JMSException("Client ID Changed", "", new Exception("Client ID Changed")));
        }
    }

    public synchronized void setAllowContextWithoutClientId(boolean allow) {
        if(!allow && Objects.nonNull(context) && (Objects.isNull(this.clientId) || this.clientId.isBlank())) {
            throw new IllegalStateException("Cannot set allowContextWithoutClientId to false while while client ID is not set");
        }
        allowContextWithoutClientId = allow;
    }

    // Context Controls
    @Override
    public synchronized void close() {
        for (JMSContextWrapper providedContext : providedContexts) {
            tryAndLogError(() -> providedContext.onException(new JMSException("Connection closing")));
        }
        providedContexts = new ArrayList<>();

        if (Objects.nonNull(context)) {
            log.info("Closing connection {}", clientId);
            tryAndLogError(context::close);
            log.info("Closed connection {}", clientId);
        }
        context = null;
        log.info("Connection context for client {} closed", clientId);
    }

    // JMS Context Methods

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
            if (Objects.isNull(clientId) && !allowContextWithoutClientId) {
                throw new NullPointerException("Client ID not set and allowContextWithoutClientId is false");
            }
            // This protects connection factory methods from blowing up from multiple connection holders
            synchronized (connectionFactory) {
                context = connectionFactory.createContext();
            }
            context.setClientID(clientId);
            context.setExceptionListener(this::onException);
            if (Objects.nonNull(clientId)) {
                log.info("Connection Context built with client ID {}", clientId);
            } else {
                log.warn("Connection Context built without client ID");
            }
            exceptionPointer.clear();
        } catch (Exception e) {
            context = null;
            if(exceptionPointer.shouldLog(e)) {
                log.error("Failed to build Main Context");
            }
            throw e;
        }
    }

    synchronized void onException(JMSException exception) {
        log.error("Connection Context expired: {}", exception.getMessage());
        close();
    }
}