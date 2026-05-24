package io.github.fishthefirst.jmscontextprovider.serde;

import jakarta.jms.Message;

import java.util.function.BiConsumer;

@FunctionalInterface
public interface MessageProcessor<T> extends BiConsumer<Message, T> {
}
