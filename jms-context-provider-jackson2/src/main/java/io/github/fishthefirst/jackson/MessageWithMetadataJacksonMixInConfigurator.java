package io.github.fishthefirst.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.fishthefirst.data.MessageWithMetadata;

import java.util.Objects;

public final class MessageWithMetadataJacksonMixInConfigurator {
    public static void configureMixIn(ObjectMapper objectMapper) {
        Objects.requireNonNull(objectMapper, "ObjectMapper cannot be null");
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.addMixIn(MessageWithMetadata.class, MessageWithMetadataMixin.class);
    }

    private MessageWithMetadataJacksonMixInConfigurator() {}
}
