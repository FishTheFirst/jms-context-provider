package io.github.fishthefirst.serde;

import io.github.fishthefirst.data.MessageWithMetadata;

import java.time.Instant;
import java.util.Objects;

public final class MessageWithMetadataMarshaller implements ObjectToStringMarshaller {
    private final ObjectToStringMarshaller objectToStringMarshaller;
    private final boolean alwaysEncodeAsString;

    public MessageWithMetadataMarshaller(ObjectToStringMarshaller objectToStringMarshaller, boolean alwaysEncodeAsString) {
        Objects.requireNonNull(objectToStringMarshaller, "Object to String Marshaller cannot be null");
        this.objectToStringMarshaller = objectToStringMarshaller;
        this.alwaysEncodeAsString = alwaysEncodeAsString;
    }

    @Override
    public String marshal(Object o) {
        try {
            String serialized = Objects.requireNonNull(objectToStringMarshaller.marshal(o), "Serializer returned null");
            return new MessageWithMetadata(Instant.now(), serialized, alwaysEncodeAsString ? String.class.getName() : o.getClass().getName()).toStringPayload();
        } catch (Exception e) {
            throw new RuntimeException("Exception thrown while serializing", e);
        }
    }

}
