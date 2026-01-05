package io.github.fishthefirst.handlers;

import java.util.function.Consumer;

@FunctionalInterface
public interface SendMessageExceptionHandler extends Consumer<Object> {}
