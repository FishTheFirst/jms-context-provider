package io.github.fishthefirst.jmscontextprovider.handlers;

@FunctionalInterface
public interface MessageCallback<T> {
    void callback(T o);
}