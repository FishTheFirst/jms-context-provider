package io.github.fishthefirst.jmscontextprovider.serde;

import io.github.fishthefirst.jmscontextprovider.data.MessageWithMetadata;

import java.time.Instant;
import java.util.Objects;

public final class MessageWithMetadataMarshaller<T> implements ObjectToStringMarshaller<T> {
    private final ObjectToStringMarshaller<T> objectToStringMarshaller;
    private final boolean alwaysEncodeAsString;

    public MessageWithMetadataMarshaller(ObjectToStringMarshaller<T> objectToStringMarshaller, boolean alwaysEncodeAsString) {
        Objects.requireNonNull(objectToStringMarshaller, "Object to String Marshaller cannot be null");
        this.objectToStringMarshaller = objectToStringMarshaller;
        this.alwaysEncodeAsString = alwaysEncodeAsString;
    }

    @Override
    public String marshal(T o) {
        try {
            String serialized = Objects.requireNonNull(objectToStringMarshaller.marshal(o), "Serializer returned null");
            return new MessageWithMetadata(Instant.now(), serialized, alwaysEncodeAsString ? String.class.getName() : o.getClass().getName()).toStringPayload();
        } catch (Exception e) {
            throw new RuntimeException("Exception thrown while serializing", e);
        }
    }

}
