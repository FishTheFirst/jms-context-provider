package io.github.fishthefirst.jmscontextprovider.data;

import java.time.Instant;

public final class MessageWithMetadata {
    private final Instant sourceDate;
    private final String payload;
    private final String className;

    public MessageWithMetadata(Instant sourceDate, String payload, String className) {
        this.sourceDate = sourceDate;
        this.payload = payload;
        this.className = className;
    }

    public MessageWithMetadata(String fromStringPayload) {
        try {
            int firstComma = fromStringPayload.indexOf(",");
            String sourceDateValue = fromStringPayload.substring("{\"sourceDate\": \"".length(), firstComma-1);
            sourceDate = Instant.parse(sourceDateValue);

            String afterFirstComma = fromStringPayload.substring(firstComma + 1);
            int secondComma = afterFirstComma.indexOf(",");
            className = afterFirstComma.substring(" \"className\": \"".length(), secondComma - 1);

            String afterSecondComma = afterFirstComma.substring(secondComma+1);
            String payloadValue = afterSecondComma.substring(" \"payload\": ".length());
            payload = payloadValue.substring(payloadValue.indexOf('\"'), payloadValue.length() - 1);
        } catch (Exception e) {
            throw new IllegalArgumentException("Message has invalid format: " + fromStringPayload, e);
        }
    }

    public Instant getSourceDate() {
        return sourceDate;
    }

    public String getPayload() {
        return payload;
    }

    public String getPayloadClassName() {
        return className;
    }

    public Class<?> getPayloadClass() throws ClassNotFoundException {
        return Class.forName(className);
    }

    public String toStringPayload() {
        return String.format("{\"sourceDate\": \"%s\", \"className\": \"%s\", \"payload\": \"%s\"}", sourceDate.toString(), className, payload);
    }
}