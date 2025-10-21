package io.github.fishthefirst;

import java.util.function.Function;

public interface StringToMessageUnmarshaller extends Function<String, MessageWithMetadata> {}