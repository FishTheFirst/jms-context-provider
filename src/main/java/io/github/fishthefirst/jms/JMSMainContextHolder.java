package io.github.fishthefirst.jms;

import io.github.fishthefirst.contextproviders.JMSContextWrapperProvider;
import io.github.fishthefirst.contextwrapper.JMSContextWrapper;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
public final class JMSMainContextHolder implements JMSContextWrapperProvider, ExceptionListener {
    private JMSContext context;
    private final List<JMSContextWrapper> providedContexts = new ArrayList<>();
    private String clientId;

    public int getSessionMode() {
        return sessionMode;
    }

    private final int sessionMode;
    private final ConnectionFactory connectionFactory;

    JMSMainContextHolder(ConnectionFactory connectionFactory, int sessionMode) {
        Objects.requireNonNull(connectionFactory, "Connection factory cannot be null");
        this.connectionFactory = connectionFactory;
        this.sessionMode = sessionMode;
    }

    public JMSContextWrapper createContext() {
        return createContext(sessionMode);
    }

    public synchronized JMSContextWrapper createContext(int sessionMode) {
        if (Objects.isNull(context)) {
            buildAndAssignContext();
        }
        JMSContextWrapper jmsContextWrapper = new JMSContextWrapper(context.createContext(sessionMode));
        providedContexts.add(jmsContextWrapper);
        return jmsContextWrapper;
    }

    private synchronized void buildAndAssignContext() {
        try {
            context = connectionFactory.createContext(sessionMode);
            context.setClientID(clientId);
            context.setExceptionListener(this);
            if(Objects.nonNull(clientId)) {
                log.info("Main Context built with client ID {}", clientId);
            }
            else {
                log.warn("Main Context built without client ID");
            }
        } catch (Exception e) {
            context = null;
            log.error("Failed to build Main Context");
            throw e;
        }
    }

    @Override
    public synchronized void onException(JMSException exception) {
        log.error("Main Context expired");
        log.error("", exception);
        close();
        try {
            buildAndAssignContext();
        } catch (Exception e2) {
            //log.error("", e2);
        }
        List<JMSContextWrapper> copyOfWrappers = List.copyOf(providedContexts);
        providedContexts.clear();
        copyOfWrappers.forEach(jmsContextWrapper -> jmsContextWrapper.onException(exception));
    }

    public synchronized void setClientId(String clientId) {
        this.clientId = clientId;
        if(Objects.nonNull(context))
            onException(new JMSException("Client ID Changed", "", new Exception("Client ID Changed")));

    }

    @Override
    public synchronized void close() {
        if(Objects.nonNull(context)) {
            try {
                context.stop();
            } catch (Exception e2) {
                log.error("", e2);
            }
            try {
                providedContexts.forEach(c->c.getContext().close());
                context.close();
            } catch (Exception e2) {
                log.error("", e2);
            }
            context = null;
            providedContexts.clear();
        }
    }
}