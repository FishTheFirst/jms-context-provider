package io.github.fishthefirst.jackson;

import com.fasterxml.jackson.annotation.JsonTypeId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

public abstract class MessageWithMetadataMixin {
    @JsonTypeId
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
    Object payload;
}
