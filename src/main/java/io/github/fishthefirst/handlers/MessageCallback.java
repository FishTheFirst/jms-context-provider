package io.github.fishthefirst.handlers;

import io.github.fishthefirst.data.MessageWithMetadata;

import java.util.function.Consumer;

public interface MessageCallback extends Consumer<MessageWithMetadata> {}