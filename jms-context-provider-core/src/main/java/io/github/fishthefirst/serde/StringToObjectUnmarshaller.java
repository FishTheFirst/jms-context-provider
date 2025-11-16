package io.github.fishthefirst.serde;

import java.util.function.BiFunction;

@FunctionalInterface
public interface StringToObjectUnmarshaller extends BiFunction<String, Class<?>, Object> {}