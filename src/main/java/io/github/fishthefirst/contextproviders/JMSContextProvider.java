package io.github.fishthefirst.contextproviders;

import jakarta.jms.JMSContext;

public interface JMSContextProvider {
    JMSContext createContext();
    JMSContext createContext(int sessionMode);
}
