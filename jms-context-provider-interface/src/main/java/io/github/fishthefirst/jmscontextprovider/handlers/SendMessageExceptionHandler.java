package io.github.fishthefirst.jmscontextprovider.handlers;

import java.util.function.Consumer;

@FunctionalInterface
public interface SendMessageExceptionHandler extends Consumer<Object> {}
