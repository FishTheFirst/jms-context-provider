package io.github.fishthefirst.jmscontextprovider.handlers;

import java.util.function.Consumer;

/**
 * This interface's implementation should avoid recursion,
 * i.e. do not call a method that can generate the exception on the same thread
 */
@FunctionalInterface
public interface SendMessageExceptionHandler extends Consumer<Object> {}
