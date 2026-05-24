package io.github.fishthefirst.jmscontextprovider.serde;

@FunctionalInterface
public interface  ObjectToStringMarshaller<T> {
    String marshal(T object);
}