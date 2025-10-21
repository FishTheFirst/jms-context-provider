package io.github.fishthefirst;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.IllegalStateException;
import jakarta.jms.JMSContext;

import java.util.Objects;

public class JMSContextProvider {
    private final JMSContext context;
    private final int sessionMode;

    public JMSContextProvider(ConnectionFactory connectionFactory, int sessionMode) {
        this.context = connectionFactory.createContext(sessionMode);
        this.sessionMode = sessionMode;
    }

    public JMSContext createContext() throws IllegalStateException {
        return createContext(sessionMode);
    }

    public JMSContext createContext(int sessionMode) throws IllegalStateException {
        if(Objects.isNull(context)) throw new IllegalStateException("Primary context not set");
        return context.createContext(sessionMode);
    }
}