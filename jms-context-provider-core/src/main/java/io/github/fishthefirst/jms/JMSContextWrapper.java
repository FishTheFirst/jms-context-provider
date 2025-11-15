package io.github.fishthefirst.jms;

import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;

import java.util.Objects;

public final class JMSContextWrapper {
    private final JMSContext context;
    private final ExceptionListener exceptionCallback;

    JMSContextWrapper(JMSContext context, ExceptionListener exceptionListener) {
        this.context = context;
        this.exceptionCallback = exceptionListener;
    }

    JMSContext getContext() {
        return context;
    }

    ExceptionListener getExceptionCallback() {
        return exceptionCallback;
    }

    void onException(JMSException exception) {
        if (Objects.nonNull(exceptionCallback)) {
            exceptionCallback.onException(exception);
        }
    }
}
