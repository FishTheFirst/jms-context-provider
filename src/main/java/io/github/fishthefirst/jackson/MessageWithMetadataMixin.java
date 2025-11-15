package io.github.fishthefirst.jackson;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.Instant;

public abstract class MessageWithMetadataMixin {

    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.WRAPPER_OBJECT, property = "@class")
    public abstract Object getPayload();

    @JsonCreator
    public MessageWithMetadataMixin(@JsonProperty("sourceDate") Instant sourceDate, @JsonProperty("payload") Object payload) {}
}
