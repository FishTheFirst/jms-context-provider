package io.github.fishthefirst.jmscontextprovider.serde;

@FunctionalInterface
public interface  ObjectToStringMarshaller {
    String marshal(Object object);
}