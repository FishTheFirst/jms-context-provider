package io.github.fishthefirst.jmscontextprovider.serde;

@FunctionalInterface
public interface StringToObjectUnmarshaller<T> {
    T unmarshal(String s);
}