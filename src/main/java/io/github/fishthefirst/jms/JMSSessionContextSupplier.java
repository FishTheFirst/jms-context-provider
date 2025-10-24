package io.github.fishthefirst.jms;

import io.github.fishthefirst.contextproviders.DynamicSessionModeJMSContextWrapperSupplier;
import io.github.fishthefirst.contextproviders.FixedSessionModeJMSContextWrapperSupplier;
import io.github.fishthefirst.contextwrapper.JMSContextWrapper;
import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSException;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;

import static io.github.fishthefirst.utils.JMSRuntimeExceptionUtils.tryAndLogError;

@Slf4j
public final class JMSSessionContextSupplier implements FixedSessionModeJMSContextWrapperSupplier, ExceptionListener {
    private JMSContextWrapper context;
    private final int sessionMode;
    private final DynamicSessionModeJMSContextWrapperSupplier contextProvider;

    JMSSessionContextSupplier(DynamicSessionModeJMSContextWrapperSupplier contextProvider, int sessionMode) {
        Objects.requireNonNull(contextProvider, "Context provider cannot be null");
        this.contextProvider = contextProvider;
        this.sessionMode = sessionMode;
    }

    @Override
    public JMSContextWrapper createContext() {
        if (Objects.isNull(context)) {
            buildAndAssignContext();
        }
        return context;
    }

    private synchronized void buildAndAssignContext() {
        try {
            JMSContextWrapper contextWrapper = contextProvider.createContext(sessionMode);
            contextWrapper.setExceptionCallback(this);
            context = new JMSContextWrapper(contextWrapper.getContext());
            log.info("Session Context built");
        } catch (Exception e) {
            log.error("Failed to build Session Context");
            context = null;
            throw e;
        }
    }

    @Override
    public synchronized void onException(JMSException exception) {
        log.error("Session Context expired: {}", exception.getMessage());
        JMSContextWrapper previousContext = context;
        tryAndLogError(this::buildAndAssignContext);
        if(Objects.nonNull(previousContext))
            previousContext.onException(exception);
    }
}