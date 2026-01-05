package io.github.fishthefirst.jms;


import io.github.fishthefirst.handlers.SendMessageExceptionHandler;
import io.github.fishthefirst.serde.MessagePreprocessor;
import io.github.fishthefirst.serde.ObjectToStringMarshaller;
import jakarta.jms.JMSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.github.fishthefirst.utils.JMSRuntimeExceptionUtils.tryAndLogError;

public final class JMSProducerTransactionManager {
    private static final Logger log = LoggerFactory.getLogger(JMSProducerTransactionManager.class);
    private static final AtomicInteger transactionId = new AtomicInteger(0);
    private final ThreadLocal<Boolean> isTransacted = new ThreadLocal<>();
    private final ThreadLocal<JMSProducer> transactionProducer = new ThreadLocal<>();
    private final ThreadLocal<List<Object>> transactedMessages = new ThreadLocal<>();
    private final JMSConnectionContextHolder connectionContextHolder;
    private final ObjectToStringMarshaller messageToStringMarshaller;
    private final SendMessageExceptionHandler sendMessageExceptionHandler;
    private final MessagePreprocessor messagePreprocessor;
    private final String destinationName;
    private final boolean topic;

    public JMSProducerTransactionManager(JMSConnectionContextHolder connectionContextHolder,
                                         ObjectToStringMarshaller messageToStringMarshaller,
                                         SendMessageExceptionHandler sendMessageExceptionHandler,
                                         MessagePreprocessor messagePreprocessor,
                                         String destinationName,
                                         boolean topic) {
        Objects.requireNonNull(connectionContextHolder, "JMS Connection Context Holder cannot be null");
        Objects.requireNonNull(connectionContextHolder, "Object to String Marshaller cannot be null");
        Objects.requireNonNull(connectionContextHolder, "JMS Connection Context Holder cannot be null");

        this.connectionContextHolder = connectionContextHolder;
        this.messageToStringMarshaller = messageToStringMarshaller;
        this.sendMessageExceptionHandler = Objects.requireNonNullElseGet(sendMessageExceptionHandler, () -> (message) -> {});
        this.messagePreprocessor = messagePreprocessor;
        this.destinationName = destinationName;
        this.topic = topic;
    }

    public void startTransaction() {
        isTransacted.set(true);
        if(Objects.isNull(transactedMessages.get())) {
            transactedMessages.set(new ArrayList<>());
        }
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
            } else {
                commit();
            }
        } catch (Exception e) {
            rollback();
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
        JMSProducer producer = Optional.ofNullable(transactionProducer.get()).orElseGet(() -> JMSContextAwareComponentFactory.createProducer(connectionContextHolder, messageToStringMarshaller, messagePreprocessor, destinationName, topic, "transacted-producer-" + transactionId.getAndIncrement(), isTransactionOpen() ? JMSContext.SESSION_TRANSACTED : JMSContext.AUTO_ACKNOWLEDGE, false));
        transactionProducer.set(producer);
        return producer;
    }

    private void tryCatch(Consumer<JMSProducer> producerMethod, String action) {
        tryAndLogError(() -> Optional.ofNullable(transactionProducer.get()).ifPresent(producerMethod), "An exception was thrown while" + action, () -> Optional.ofNullable(transactedMessages.get()).ifPresent(list -> list.forEach(this::messageFailedCallback)));
    }

    private void messageFailedCallback(Object failedMessage) {
        try {
            sendMessageExceptionHandler.accept(failedMessage);
        } catch (Exception sendMessageExceptionHandlerException) {
            log.error("Send Message Exception Handler threw an exception", sendMessageExceptionHandlerException);
        }
    }

    private boolean isTransactionOpen() {
        return Optional.ofNullable(isTransacted.get()).orElse(false);
    }

    private void clearThreadLocals() {
        Optional.ofNullable(transactionProducer.get()).ifPresent(JMSProducer::close);
        transactionProducer.remove();
        isTransacted.remove();
        transactedMessages.remove();
    }
}
