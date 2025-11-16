package io.github.fishthefirst.serde;

import java.util.function.Function;

@FunctionalInterface
public interface ObjectToStringMarshaller extends Function<Object, String> {}