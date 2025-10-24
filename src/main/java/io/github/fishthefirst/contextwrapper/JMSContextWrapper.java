package io.github.fishthefirst.contextwrapper;

import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class JMSContextWrapper implements ExceptionListener {
    private final JMSContext context;
    private final AtomicReference<ExceptionListener> exceptionCallback = new AtomicReference<>();

    public JMSContextWrapper(JMSContext context) {
        this.context = context;
    }

    public JMSContext getContext() {
        return context;
    }

    @Override
    public void onException(JMSException exception) {
        ExceptionListener exceptionListener = this.exceptionCallback.get();
        if(Objects.nonNull(exceptionListener))
            exceptionListener.onException(exception);
    }

    public void setExceptionCallback(ExceptionListener listener) {
        this.exceptionCallback.set(listener);
    }
}
