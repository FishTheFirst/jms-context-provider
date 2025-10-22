package io.github.fishthefirst.data;

import java.time.Instant;

public final class MessageWithMetadata {
    private final Instant sourceDate;
    private final Object payload;

    public MessageWithMetadata(Instant sourceDate, Object payload) {
        this.sourceDate = sourceDate;
        this.payload = payload;
    }

    public Instant getSourceDate() {
        return sourceDate;
    }

    public Object getPayload() {
        return payload;
    }
}