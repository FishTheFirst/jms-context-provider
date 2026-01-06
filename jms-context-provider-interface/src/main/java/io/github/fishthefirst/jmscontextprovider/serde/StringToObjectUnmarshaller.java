package io.github.fishthefirst.jmscontextprovider.serde;

@FunctionalInterface
public interface StringToObjectUnmarshaller {
    Object unmarshal(String s);
}