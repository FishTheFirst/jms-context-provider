package io.github.fishthefirst.serde;

@FunctionalInterface
public interface  ObjectToStringMarshaller {
    String marshal(Object object);
}