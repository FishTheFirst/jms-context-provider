package io.github.fishthefirst.handlers;

import java.util.function.Consumer;

@FunctionalInterface
public interface MessageCallback extends Consumer<Object> {}