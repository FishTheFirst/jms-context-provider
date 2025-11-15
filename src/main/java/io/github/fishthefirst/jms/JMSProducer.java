package io.github.fishthefirst.jms;

import io.github.fishthefirst.data.MessageWithMetadata;
import io.github.fishthefirst.serde.MessagePreprocessor;
import io.github.fishthefirst.serde.MessageToStringMarshaller;
import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.Objects;

@Slf4j
public final class JMSProducer {
    private final MessageToStringMarshaller messageToStringMarshaller;
    private final MessagePreprocessor messagePreprocessor;
    private final JMSSessionContextSupplier contextSupplier;

    private final boolean isTopic;
    private final boolean keepAlive;
    private final Integer messageTimeToLive;
    private final String destinationName;
    private final String producerName;

    private JMSContext context;
    private jakarta.jms.JMSProducer jmsProducer;
    private Destination destination;

    JMSProducer(JMSSessionContextSupplier contextSupplier,
                MessageToStringMarshaller messageToStringMarshaller,
                MessagePreprocessor messagePreprocessor,
                String destinationName,
                boolean isTopic,
                String producerName,
                Integer messageTimeToLive,
                boolean keepAlive) {
        this.messageToStringMarshaller = messageToStringMarshaller;
        this.messagePreprocessor = messagePreprocessor;
        this.contextSupplier = contextSupplier;
        this.isTopic = isTopic;
        this.destinationName = destinationName;
        this.producerName = producerName;
        this.messageTimeToLive = messageTimeToLive;
        this.keepAlive = keepAlive;
    }

    public synchronized void sendMessage(Object o) {
        if(Objects.isNull(context)) {
            createProducer();
        }
        TextMessage textMessage = context.createTextMessage(serialize(o));
        preprocessMessage(textMessage);
        jmsProducer.send(destination, textMessage);
    }

    private void preprocessMessage(TextMessage textMessage) {
        try {
            messagePreprocessor.accept(textMessage);
        } catch (Exception e) {
            throw new RuntimeException("Message preprocessor threw an exception", e);
        }
    }

    private void createProducer() {
        JMSContextWrapper contextWrapper = contextSupplier.createContext(this::onException);
        this.context = contextWrapper.getContext();
        this.jmsProducer = context.createProducer();
        if(Objects.nonNull(messageTimeToLive)) {
            this.jmsProducer.setTimeToLive(messageTimeToLive);
        }
        destination = isTopic ? context.createTopic(destinationName) : context.createQueue(destinationName);
    }

    private String serialize(Object o) {
        try {
            MessageWithMetadata messageWithMetadata = new MessageWithMetadata(Instant.now(), o);
            String serialized = messageToStringMarshaller.apply(messageWithMetadata);
            return Objects.requireNonNull(serialized, "Serializer returned null");
        } catch (Exception e) {
            throw new RuntimeException("Exception thrown while serializing", e);
        }
    }

    private synchronized void onException(JMSException e) {
        close();
    }

    synchronized void commit() {
        Objects.requireNonNull(context, "Call to commit without a context");
        if(context.getTransacted()) {
            context.commit();
        }
        if(!keepAlive) {
            close();
        }
    }

    synchronized void rollback() {
        Objects.requireNonNull(context, "Call to rollback without a context");
        if(context.getTransacted()) {
            context.rollback();
        }
        if(!keepAlive) {
            close();
        }
    }

    public void close() {
        if(Objects.nonNull(context)) {
            try {
                context.close();
            } catch (Exception e) {
                log.error("An exception was thrown while closing JMS Producer {} context", producerName);
            }
        }
        context = null;
        jmsProducer = null;
        destination = null;
    }
}
