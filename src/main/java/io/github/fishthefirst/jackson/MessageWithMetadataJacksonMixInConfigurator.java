package io.github.fishthefirst.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.fishthefirst.data.MessageWithMetadata;

import java.util.Objects;

public final class MessageWithMetadataJacksonMixInConfigurator {
    public static void configureMixIn(ObjectMapper objectMapper) {
        Objects.requireNonNull(objectMapper, "ObjectMapper cannot be null");
        objectMapper.addMixIn(MessageWithMetadata.class, MessageWithMetadataMixin.class);
    }

    private MessageWithMetadataJacksonMixInConfigurator() {
    }
}
