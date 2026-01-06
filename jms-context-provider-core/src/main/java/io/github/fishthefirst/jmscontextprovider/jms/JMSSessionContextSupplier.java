package io.github.fishthefirst.jmscontextprovider.jms;

import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static io.github.fishthefirst.jmscontextprovider.utils.JMSRuntimeExceptionUtils.tryAndLogError;

public final class JMSSessionContextSupplier {
    private static final Logger log = LoggerFactory.getLogger(JMSSessionContextSupplier.class);

    // Constructor vars
    private final JMSConnectionContextHolder contextProvider;
    private final int sessionMode;

    // JMS
    private JMSContextWrapper context;

    JMSSessionContextSupplier(JMSConnectionContextHolder contextProvider, int sessionMode) {
        Objects.requireNonNull(contextProvider, "Context provider cannot be null");
        this.contextProvider = contextProvider;
        this.sessionMode = sessionMode;
    }

    synchronized JMSContextWrapper createContext(ExceptionListener exceptionListener) {
        if (Objects.isNull(context)) {
            buildAndAssignContext(exceptionListener);
        }
        return context;
    }

    private synchronized void buildAndAssignContext(ExceptionListener exceptionListener) {
        try {
            JMSContextWrapper contextWrapper = contextProvider.createContext(sessionMode, this::onException);
            context = new JMSContextWrapper(contextWrapper.getContext(), exceptionListener);
            log.info("Session Context built");
        } catch (Exception e) {
            log.error("Failed to build Session Context");
            context = null;
            throw e;
        }
    }

    private synchronized void onException(JMSException exception) {
        log.error("Session Context expired: {}", exception.getMessage());
        if (Objects.nonNull(context)) {
            tryAndLogError(context.getContext()::close);
        }
        JMSContextWrapper previousContext = context;
        context = null;
        if (Objects.nonNull(previousContext)) {
            previousContext.onException(exception);
        }
    }
}