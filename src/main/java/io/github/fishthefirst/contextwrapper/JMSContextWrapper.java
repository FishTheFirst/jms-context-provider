package io.github.fishthefirst.contextwrapper;

import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;

public class JMSContextWrapper implements ExceptionListener {
    private final JMSContext context;
    private ExceptionListener exceptionCallback;
    public JMSContextWrapper(JMSContext context) {
        this.context = context;
    }

    public JMSContext getContext() {
        return context;
    }

    @Override
    public void onException(JMSException exception) {
        this.exceptionCallback.onException(exception);
    }

    public void setExceptionCallback(ExceptionListener listener) {
        this.exceptionCallback = listener;
    }
}
