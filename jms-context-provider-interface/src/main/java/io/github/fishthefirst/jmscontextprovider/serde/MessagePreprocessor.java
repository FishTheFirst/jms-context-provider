package io.github.fishthefirst.jmscontextprovider.serde;

import jakarta.jms.Message;

import java.util.function.Consumer;

@FunctionalInterface
public interface MessagePreprocessor extends Consumer<Message> {
}
