package io.github.fishthefirst.jms;

import io.github.fishthefirst.handlers.SendMessageExceptionHandler;
import io.github.fishthefirst.serde.MessageToStringMarshaller;
import jakarta.jms.JMSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public final class JMSProducerTransactionManager {
    private static final Logger log = LoggerFactory.getLogger(JMSProducerTransactionManager.class);
    private static final AtomicInteger transactionId = new AtomicInteger(0);
    private final ThreadLocal<Boolean> isTransacted = new ThreadLocal<>();
    private final ThreadLocal<JMSProducer> transactionProducer = new ThreadLocal<>();
    private final ThreadLocal<List<Object>> transactedMessages = new ThreadLocal<>();
    private final JMSConnectionContextHolder connectionContextHolder;
    private final MessageToStringMarshaller messageToStringMarshaller;
    private final SendMessageExceptionHandler sendMessageExceptionHandler;
    private final String destinationName;
    private final JMSProducer transactionlessProducer;
    private final boolean topic;

    public JMSProducerTransactionManager(JMSConnectionContextHolder connectionContextHolder,
                                         MessageToStringMarshaller messageToStringMarshaller,
                                         SendMessageExceptionHandler sendMessageExceptionHandler,
                                         String destinationName,
                                         boolean topic) {
        this.connectionContextHolder = connectionContextHolder;
        this.messageToStringMarshaller = messageToStringMarshaller;
        this.sendMessageExceptionHandler = sendMessageExceptionHandler;
        this.destinationName = destinationName;
        this.topic = topic;
        transactionlessProducer = JMSContextAwareComponentFactory.createProducer(connectionContextHolder, messageToStringMarshaller, destinationName, topic, "transactionless-producer-" + transactionId.getAndIncrement(), JMSContext.AUTO_ACKNOWLEDGE, true);
    }

    public void startTransaction() {
        isTransacted.set(true);
        transactedMessages.set(new ArrayList<>());
    }

    public void sendObjectsTransacted(Iterable<Object> objects) {
        startTransaction();
        Iterator<Object> iterator = objects.iterator();
        while (isTransactionOpen() && iterator.hasNext()) {
            sendObject(iterator.next());
        }
        if (isTransactionOpen()) {
            commit();
            return;
        }
        while (iterator.hasNext()) {
            messageFailedCallback(iterator.next());
        }
    }

    public void sendObject(Object object) {
        JMSProducer jmsProducer = getProducerForMessage();
        try{
            jmsProducer.sendMessage(object);
            if(isTransactionOpen()) {
                transactedMessages.get().add(object);
            }
        } catch (Exception e) {
            if(isTransactionOpen()) {
                rollback();
            }
            messageFailedCallback(object);
        }
    }

    public void commit() {
        tryCatch(JMSProducer::commit, "committing");
        clearThreadLocals();
    }

    public void rollback() {
        tryCatch(JMSProducer::rollback, "rolling back");
        Optional.ofNullable(transactedMessages.get()).ifPresent(list -> list.forEach(this::messageFailedCallback));
        clearThreadLocals();
    }

    private JMSProducer getProducerForMessage() {
        return !isTransactionOpen() ? transactionlessProducer :
             Optional.ofNullable(transactionProducer.get()).orElseGet(() -> {
                JMSProducer producer = JMSContextAwareComponentFactory.createProducer(connectionContextHolder, messageToStringMarshaller, destinationName, topic, "transacted-producer-" + transactionId.getAndIncrement(), JMSContext.SESSION_TRANSACTED, false);
                transactionProducer.set(producer);
                return producer;
             });
    }

    private void tryCatch(Consumer<JMSProducer> producerMethod, String action) {
        try {
            Optional.ofNullable(transactionProducer.get()).ifPresent(producerMethod);
        } catch (Exception e) {
            log.error("An exception was thrown while {}", action, e);
            if(!isTransactionOpen()) return;
            transactedMessages.get().forEach(this::messageFailedCallback);
        }
    }

    private void messageFailedCallback(Object failedMessage) {
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
        Optional.ofNullable(transactionProducer.get()).ifPresent(JMSProducer::close);
        transactionProducer.remove();
        isTransacted.remove();
        transactedMessages.remove();
    }

    public void close() {
        transactionlessProducer.close();
    }
}
