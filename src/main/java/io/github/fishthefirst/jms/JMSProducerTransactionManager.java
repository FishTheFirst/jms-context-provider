package io.github.fishthefirst.jms;

import io.github.fishthefirst.handlers.SendMessageExceptionHandler;
import io.github.fishthefirst.serde.ObjectToStringMarshaller;
import jakarta.jms.JMSContext;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Slf4j
public final class JMSProducerTransactionManager {
    private static final AtomicInteger transactionId = new AtomicInteger(0);
    private final ThreadLocal<Boolean> isTransacted = new ThreadLocal<>();
    private final ThreadLocal<JMSProducer> transactionProducer = new ThreadLocal<>();
    private final ThreadLocal<List<String>> transactedMessages = new ThreadLocal<>();
    private final JMSConnectionContextHolder connectionContextHolder;
    private final ObjectToStringMarshaller objectToStringMarshaller;
    private final SendMessageExceptionHandler sendMessageExceptionHandler;
    private final String destinationName;
    private final JMSProducer transactionlessProducer;
    private final boolean topic;

    public JMSProducerTransactionManager(JMSConnectionContextHolder connectionContextHolder,
                                         ObjectToStringMarshaller objectToStringMarshaller,
                                         SendMessageExceptionHandler sendMessageExceptionHandler,
                                         String destinationName,
                                         boolean topic) {
        this.connectionContextHolder = connectionContextHolder;
        this.objectToStringMarshaller = objectToStringMarshaller;
        this.sendMessageExceptionHandler = sendMessageExceptionHandler;
        this.destinationName = destinationName;
        this.topic = topic;
        transactionlessProducer = JMSContextAwareComponentFactory.createProducer(connectionContextHolder, objectToStringMarshaller, destinationName, topic, "transactionless-producer-" + transactionId.getAndIncrement(), JMSContext.AUTO_ACKNOWLEDGE, true);
    }

    public void startTransaction() {
        isTransacted.set(true);
        transactedMessages.set(new ArrayList<>());
    }

    public void sendObject(Object o) {
        JMSProducer jmsProducer = getProducerForMessage();
        jmsProducer.sendMessage(o);
    }

    public void sendMessage(String s) {
        JMSProducer jmsProducer = getProducerForMessage();
        try{
            jmsProducer.sendMessage(s);
            if(isTransactionOpen()) {
                transactedMessages.get().add(s);
            }
        } catch (Exception e) {
            rollback();
        }
    }

    public void commit() {
        tryCatch(JMSProducer::commit, "committing");
        clearThreadLocals();
    }

    public void rollback() {
        tryCatch(JMSProducer::rollback, "rolling back");
        transactedMessages.get().forEach(this::messageFailedCallback);
        clearThreadLocals();
    }

    private JMSProducer getProducerForMessage() {
        return !(isTransactionOpen()) ? transactionlessProducer :
             Optional.ofNullable(transactionProducer.get()).orElseGet(() -> {
                JMSProducer producer = JMSContextAwareComponentFactory.createProducer(connectionContextHolder, objectToStringMarshaller, destinationName, topic, "transacted-producer-" + transactionId.getAndIncrement(), JMSContext.SESSION_TRANSACTED, false);
                transactionProducer.set(producer);
                return producer;
             });
    }

    private void tryCatch(Consumer<JMSProducer> producerMethod, String action) {
        try {
            Optional.ofNullable(transactionProducer.get()).ifPresent(producerMethod);
        } catch (Exception e) {
            log.error("An exception was thrown while {}", action, e);
            if(!(isTransactionOpen())) return;
            transactedMessages.get().forEach(this::messageFailedCallback);
        }
    }

    private void messageFailedCallback(String failedMessage) {
        try {
            sendMessageExceptionHandler.accept(failedMessage);
        } catch (Exception sendMessageExceptionHandlerException) {
            log.error("Send Message Exception Handler threw an exception", sendMessageExceptionHandlerException);
        }
    }

    private boolean isTransactionOpen() {
        Optional<Boolean> isTransactedOptional = Optional.ofNullable(isTransacted.get());
        return isTransactedOptional.isPresent() && isTransactedOptional.get();
    }

    private void clearThreadLocals() {
        transactionProducer.remove();
        isTransacted.remove();
        transactedMessages.remove();
    }
}
