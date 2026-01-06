package io.github.fishthefirst.jmscontextprovider.handlers;

import java.util.function.Consumer;

@FunctionalInterface
public interface MessageCallback extends Consumer<Object> {}