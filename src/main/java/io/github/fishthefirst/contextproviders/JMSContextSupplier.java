package io.github.fishthefirst.contextproviders;

import jakarta.jms.JMSContext;

public interface JMSContextSupplier {
    JMSContext createContext();
    JMSContext createContext(int sessionMode);
}
