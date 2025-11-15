package io.github.fishthefirst.serde;

import io.github.fishthefirst.data.MessageWithMetadata;

import java.util.function.Function;

@FunctionalInterface
public interface StringToMessageUnmarshaller extends Function<String, MessageWithMetadata> {}