package io.github.fishthefirst.jmscontextprovider.jms;

import io.github.fishthefirst.jmscontextprovider.exceptions.ExceptionPointer;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import static io.github.fishthefirst.jmscontextprovider.utils.JMSRuntimeExceptionUtils.tryAndLogError;

public final class JMSSessionContextSupplier {
    private static final Logger log = LoggerFactory.getLogger(JMSSessionContextSupplier.class);

    // Constructor vars
    private final JMSConnectionContextHolder contextProvider;
    private final int sessionMode;
    private final ExceptionPointer exceptionPointer = new ExceptionPointer(60000);
    private final ReentrantLock sessionBusy = new ReentrantLock();

    // JMS
    private JMSContextWrapper context;

    JMSSessionContextSupplier(JMSConnectionContextHolder contextProvider, int sessionMode) {
        Objects.requireNonNull(contextProvider, "Context provider cannot be null");
        this.contextProvider = contextProvider;
        this.sessionMode = sessionMode;
    }

    JMSContext createContext(ExceptionListener exceptionListener) {
        if(!sessionBusy.tryLock()) throw new ConcurrentModificationException("Session is busy");
        try {
            if (Objects.isNull(context)) {
                buildAndAssignContext(exceptionListener);
            }
            else {
                JMSContext realContext = context.getContext();
                try {
                    realContext.recover();
                } catch (JMSRuntimeException e) {
                    realContext.close();
                    throw e;
                }
            }
            return context.getContext();
        }  finally {
            sessionBusy.unlock();
        }
    }

    void release() {
        try {
            sessionBusy.lock();
            if (Objects.nonNull(context)) {
                tryAndLogError(context.getContext()::close);
            }
            context = null;
        } finally {
            sessionBusy.unlock();
        }
    }

    private void buildAndAssignContext(ExceptionListener exceptionListener) {
        try {
            // Create a session context from the connection context
            JMSContextWrapper contextWrapper = contextProvider.createContext(sessionMode, this::onException);
            // Wrap the session context with the caller's exception handler,
            // so we know where to propagate connection exceptions to
            context = new JMSContextWrapper(contextWrapper.getContext(), exceptionListener);
            log.info("Session Context built");
            exceptionPointer.clear();
        } catch (Exception e) {
            if(exceptionPointer.shouldLog(e)) {
                log.error("Failed to build Session Context");
            }
            context = null;
            throw e;
        }
    }

    void onException(JMSException exception) {
        try {
            sessionBusy.lock();
            if (Objects.nonNull(context)) {
                log.error("Session Context expired: {}", exception.getMessage());
                context.onException(exception);
                if (Objects.nonNull(context)) {
                    tryAndLogError(context.getContext()::close);
                }
            }
            context = null;
        } finally {
            sessionBusy.unlock();
        }
    }
}