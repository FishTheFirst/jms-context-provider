package io.github.fishthefirst.data;

import org.junit.jupiter.api.Test;

import java.time.Instant;

class MessageWithMetadataTest {

    @Test
    void toStringPayload() throws ClassNotFoundException {
        MessageWithMetadata messageWithMetadata = new MessageWithMetadata(Instant.now(), "", String.class.getName());
        String stringPayload = messageWithMetadata.toStringPayload();
        MessageWithMetadata recvMessage = new MessageWithMetadata(stringPayload);

        Class<?> recvClass = recvMessage.getPayloadClass();
        if(recvClass == String.class) {
            new String(recvMessage.getPayload());
        }
    }
}