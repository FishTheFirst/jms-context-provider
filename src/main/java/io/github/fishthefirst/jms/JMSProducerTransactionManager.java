package io.github.fishthefirst.jms;

import io.github.fishthefirst.serde.ObjectToStringMarshaller;
import jakarta.jms.JMSContext;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public final class JMSProducerTransactionManager {
    private static final AtomicInteger transactionId = new AtomicInteger(0);
    private final ThreadLocal<Boolean> isTransacted = new ThreadLocal<>();
    private final ThreadLocal<JMSProducer> transactionProducer = new ThreadLocal<>();
    private final JMSConnectionContextHolder connectionContextHolder;
    private final ObjectToStringMarshaller objectToStringMarshaller;
    private final String destinationName;
    private final JMSProducer transactionlessProducer;
    private final boolean topic;

    public JMSProducerTransactionManager(JMSConnectionContextHolder connectionContextHolder,
                                         ObjectToStringMarshaller objectToStringMarshaller,
                                         String destinationName,
                                         boolean topic) {
        this.connectionContextHolder = connectionContextHolder;
        this.objectToStringMarshaller = objectToStringMarshaller;
        this.destinationName = destinationName;
        this.topic = topic;
        transactionlessProducer = JMSContextAwareComponentFactory.createProducer(connectionContextHolder, objectToStringMarshaller, destinationName, topic, "transactionless-producer-" + transactionId.getAndIncrement(), JMSContext.AUTO_ACKNOWLEDGE, true);
    }

    public void startTransaction() {
        isTransacted.set(true);
    }

    public void sendObject(Object o) {
        JMSProducer jmsProducer = getProducerForMessage();
        jmsProducer.sendMessage(o);
    }

    public void sendMessage(String s) {
        JMSProducer jmsProducer = getProducerForMessage();
        jmsProducer.sendMessage(s);
    }

    public void commit() {
        Optional.ofNullable(transactionProducer.get()).ifPresent(JMSProducer::commit);
        transactionProducer.remove();
        isTransacted.remove();
    }

    public void rollback() {
        Optional.ofNullable(transactionProducer.get()).ifPresent(JMSProducer::rollback);
        transactionProducer.remove();
        isTransacted.remove();
    }

    private JMSProducer getProducerForMessage() {
        Optional<Boolean> isTransactedOptional = Optional.ofNullable(isTransacted.get());
        boolean transacted = isTransactedOptional.isPresent() && isTransactedOptional.get();
        if(transacted) {
            return Optional.ofNullable(transactionProducer.get()).orElseGet(() -> {
                JMSProducer producer = JMSContextAwareComponentFactory.createProducer(connectionContextHolder, objectToStringMarshaller, destinationName, topic, "transacted-producer-" + transactionId.getAndIncrement(), JMSContext.SESSION_TRANSACTED, false);
                transactionProducer.set(producer);
                return producer;
            });
        }
        return transactionlessProducer;
    }
}
