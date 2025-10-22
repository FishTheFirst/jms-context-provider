package io.github.fishthefirst.contextproviders;

import jakarta.jms.JMSContext;

import java.util.function.Supplier;

public interface FixedSessionModeJMSContextProvider extends Supplier<JMSContext> {
}
