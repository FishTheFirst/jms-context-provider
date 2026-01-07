package io.github.fishthefirst.jmscontextprovider.jms;

import io.github.fishthefirst.jmscontextprovider.serde.MessagePreprocessor;
import io.github.fishthefirst.jmscontextprovider.serde.ObjectToStringMarshaller;
import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public final class JMSProducer {
    private static final Logger log = LoggerFactory.getLogger(JMSProducer.class);

    private final ObjectToStringMarshaller objectToStringMarshaller;
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
                ObjectToStringMarshaller objectToStringMarshaller,
                String destinationName,
                boolean isTopic,
                String producerName,
                int messageTimeToLive,
                boolean keepAlive) {
        this(contextSupplier, objectToStringMarshaller, null, destinationName, isTopic, producerName, messageTimeToLive, keepAlive);
    }

    JMSProducer(JMSSessionContextSupplier contextSupplier,
                ObjectToStringMarshaller objectToStringMarshaller,
                MessagePreprocessor messagePreprocessor,
                String destinationName,
                boolean isTopic,
                String producerName,
                int messageTimeToLive,
                boolean keepAlive) {
        Objects.requireNonNull(objectToStringMarshaller, "Object to String Marshaller cannot be null");
        Objects.requireNonNull(contextSupplier, "JMS Session Context Supplier cannot be null");
        Objects.requireNonNull(destinationName, "Destination name cannot be null nor blank");
        if(destinationName.isBlank()) throw new IllegalArgumentException("Destination name cannot be null nor blank");
        Objects.requireNonNull(destinationName, "Producer name cannot be null nor blank");
        if(destinationName.isBlank()) throw new IllegalArgumentException("Producer name cannot be null nor blank");
        this.objectToStringMarshaller = objectToStringMarshaller;
        this.messagePreprocessor = Objects.requireNonNullElseGet(messagePreprocessor, () -> message -> {});
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
            return Objects.requireNonNull(objectToStringMarshaller.marshal(o), "Serializer returned null");
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

    public boolean isAlive() {
        return Objects.nonNull(context) && Objects.nonNull(jmsProducer) && Objects.nonNull(destination);
    }
}
