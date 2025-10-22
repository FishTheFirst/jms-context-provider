package io.github.fishthefirst.contextproviders;

import jakarta.jms.JMSContext;

import java.util.function.Function;

public interface DynamicSessionModeJMSContextSupplier extends Function<Integer, JMSContext> {
}
