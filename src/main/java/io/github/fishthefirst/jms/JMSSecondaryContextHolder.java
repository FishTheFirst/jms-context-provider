package io.github.fishthefirst.jms;

import io.github.fishthefirst.contextproviders.JMSContextProvider;
import io.github.fishthefirst.contextproviders.JMSContextWrapperProvider;
import io.github.fishthefirst.contextwrapper.JMSContextWrapper;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

@Slf4j
public final class JMSSecondaryContextHolder implements JMSContextProvider, ExceptionListener {
    private JMSContext context;
    private final int sessionMode;
    private final JMSContextWrapperProvider contextProvider;

    JMSSecondaryContextHolder(JMSContextWrapperProvider contextProvider, int sessionMode) {
        Objects.requireNonNull(contextProvider, "Connection factory cannot be null");
        this.contextProvider = contextProvider;
        this.sessionMode = sessionMode;
    }

    public JMSContext createContext() {
        return createContext(sessionMode);
    }

    public synchronized JMSContext createContext(int sessionMode) {
        if (Objects.isNull(context)) {
            buildAndAssignContext();
        }
        return context;
    }

    private synchronized void buildAndAssignContext() {
        try {
            JMSContextWrapper contextWrapper = contextProvider.createContext(sessionMode);
            contextWrapper.setExceptionCallback(this);
            context = contextWrapper.getContext();
            log.info("Secondary Context built");
        } catch (Exception e) {
            log.error("Failed to build Secondary Context");
            context = null;
            throw e;
        }
    }

    @Override
    public synchronized void onException(JMSException exception) {
        log.error("Secondary Context expired");
        log.error("", exception);
        try {
            buildAndAssignContext();
        } catch (Exception e2) {
            log.error("", e2);
        }
    }
}