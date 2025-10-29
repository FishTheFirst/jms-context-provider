package io.github.fishthefirst.serde;

import jakarta.jms.TextMessage;

import java.util.function.Consumer;

@FunctionalInterface
public interface MessagePreprocessor extends Consumer<TextMessage> {
}
