package io.github.fishthefirst.serde;

@FunctionalInterface
public interface StringToObjectUnmarshaller {
    Object unmarshal(String s);
}