package io.github.fishthefirst.jmscontextprovider.jms;


import io.github.fishthefirst.jmscontextprovider.handlers.SendMessageAbortedHandler;
import io.github.fishthefirst.jmscontextprovider.handlers.SendMessageExceptionHandler;
import io.github.fishthefirst.jmscontextprovider.serde.MessageProcessor;
import io.github.fishthefirst.jmscontextprovider.serde.ObjectToStringMarshaller;
import io.github.fishthefirst.jmscontextprovider.utils.CustomizableThreadFactory;
import jakarta.jms.JMSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.github.fishthefirst.jmscontextprovider.utils.JMSRuntimeExceptionUtils.tryAndLogError;

public final class JMSProducerTransactionManager<T> {
    private static final Logger log = LoggerFactory.getLogger(JMSProducerTransactionManager.class);
    private static final AtomicLong transactionId = new AtomicLong(0);
    private final ThreadLocal<Boolean> isTransacted = new ThreadLocal<>();
    private final ThreadLocal<JMSProducer<T>> transactionProducer = new ThreadLocal<>();
    private final ThreadLocal<List<T>> transactedMessages = new ThreadLocal<>();
    private final ThreadLocal<Boolean> transactionAlreadyFailed = new ThreadLocal<>();

    private final JMSConnectionContextHolder connectionContextHolder;
    private final ObjectToStringMarshaller<T> messageToStringMarshaller;
    private final SendMessageExceptionHandler<T> sendMessageExceptionHandler;
    private final SendMessageAbortedHandler<T> sendMessageAbortedHandler;
    private final MessageProcessor<T> messagePreProcessor;
    private final MessageProcessor<T> messagePostProcessor;

    public String getDestinationName() {
        return destinationName;
    }

    private final String destinationName;
    private final boolean topic;

    private final ThreadPoolExecutor executor;

    public JMSProducerTransactionManager(JMSConnectionContextHolder connectionContextHolder,
                                         ObjectToStringMarshaller<T> messageToStringMarshaller,
                                         SendMessageExceptionHandler<T> sendMessageExceptionHandler,
                                         SendMessageAbortedHandler<T> sendMessageAbortedHandler,
                                         MessageProcessor<T> messagePreProcessor,
                                         MessageProcessor<T> messagePostProcessor,
                                         String destinationName,
                                         boolean topic) {

        Objects.requireNonNull(connectionContextHolder, "JMS Connection Context Holder cannot be null");
        Objects.requireNonNull(messageToStringMarshaller, "Object to String Marshaller cannot be null");

        this.connectionContextHolder = connectionContextHolder;
        this.messageToStringMarshaller = messageToStringMarshaller;
        this.sendMessageExceptionHandler = Objects.requireNonNullElseGet(sendMessageExceptionHandler, () -> (message) -> {});
        this.sendMessageAbortedHandler = Objects.requireNonNullElseGet(sendMessageAbortedHandler, () -> (message) -> {});
        this.messagePreProcessor = messagePreProcessor;
        this.messagePostProcessor = messagePostProcessor;
        this.destinationName = destinationName;
        this.topic = topic;
        // TODO How to enable executor configuration
        this.executor = new ThreadPoolExecutor(
                0,
                8,
                30,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1000),
                CustomizableThreadFactory.getInstance(this),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void startTransaction() {
        isTransacted.set(true);
        if (Objects.isNull(transactedMessages.get())) {
            transactedMessages.set(new ArrayList<>());
        }
    }

    public void sendObjectsTransacted(Iterable<T> objects) {
        startTransaction();
        Iterator<T> iterator = objects.iterator();
        while (!hasTransactionFailed() && iterator.hasNext()) {
            sendObject(iterator.next());
        }
        if (!hasTransactionFailed()) {
            commit();
            return;
        }
        while (iterator.hasNext()) {
            messageFailedCallback(iterator.next());
        }
    }

    public void sendObject(T object) {
        boolean transactionOpen = isTransactionOpen();
        List<T> transactedMessagesList = transactedMessages.get();

        if(transactionOpen && (hasTransactionFailed() || (!transactedMessagesList.isEmpty() && !isProducerAlive()))) {
            messageFailedCallback(object);
            return;
        }

        JMSProducer<T> jmsProducer = getProducerForMessage();
        try {
            jmsProducer.sendMessage(object);
            if (transactionOpen) {
                transactedMessagesList.add(object);
            } else {
                commit();
            }
        } catch (Exception e) {
            if (transactionOpen) {
                transactionAlreadyFailed.set(true);
            }
            rollback();
            messageFailedCallback(object);
        }
    }

    private Boolean isProducerAlive() {
        return Optional.ofNullable(transactionProducer.get()).map(JMSProducer::isAlive).orElse(false);
    }

    public void commit() {
        if(hasTransactionFailed()) {
            rollback();
        }
        else {
            tryCatch(JMSProducer::commit, "committing");
        }
        clearThreadLocals();
    }

    public void commitAsync() {
        try {
            JMSProducer<T> jmsProducer = transactionProducer.get();
            List<T> transactedMessagesList = transactedMessages.get();
            Boolean isTransactedC = isTransacted.get();
            Boolean hasTransactionFailed = transactionAlreadyFailed.get();
            executor.submit(() -> {
                setThreadLocals(jmsProducer, transactedMessagesList, isTransactedC, hasTransactionFailed);
                commit();
            });
        } finally {
            // This only removes this thread's locals and not the new thread's locals
            clearThreadLocalsNoClose();
        }
    }

    public void rollback() {
        tryCatch(JMSProducer::rollback, "rolling back");
        Optional.ofNullable(transactedMessages.get()).ifPresent(list -> list.forEach(this::messageFailedCallback));
    }

    public void abort() {
        tryCatch(JMSProducer::rollback, "rolling back");
        Optional.ofNullable(transactedMessages.get()).ifPresent(list -> list.forEach(this::messageAbortedCallback));
    }

    private void setThreadLocals(JMSProducer<T> producer,
                                 List<T> transactedMessages,
                                 Boolean isTransacted,
                                 Boolean hasTransactionFailed) {
        this.transactionProducer.set(producer);
        this.transactedMessages.set(transactedMessages);
        this.isTransacted.set(isTransacted);
        this.transactionAlreadyFailed.set(hasTransactionFailed);
    }

    private JMSProducer<T> getProducerForMessage() {
        JMSProducer<T> producer = Optional
                .ofNullable(transactionProducer.get())
                .orElseGet(() ->
                        JMSContextAwareComponentFactory
                                .createProducer(
                                        connectionContextHolder,
                                        messageToStringMarshaller,
                                        messagePreProcessor,
                                        messagePostProcessor,
                                        destinationName,
                                        topic,
                                        "transacted-producer-" + transactionId.getAndIncrement(),
                                        isTransactionOpen() ?
                                                JMSContext.SESSION_TRANSACTED : JMSContext.AUTO_ACKNOWLEDGE,
                                        false));
        transactionProducer.set(producer);
        return producer;
    }

    private void tryCatch(Consumer<JMSProducer<T>> producerMethod, String action) {
        tryAndLogError(() -> Optional
                        .ofNullable(transactionProducer.get())
                        .ifPresent(producerMethod), "An exception was thrown while " + action,
                () -> Optional.ofNullable(transactedMessages.get()).ifPresent(list -> list.forEach(this::messageFailedCallback)));
    }

    private void messageFailedCallback(T failedMessage) {
        try {
            sendMessageExceptionHandler.accept(failedMessage);
        } catch (Exception sendMessageExceptionHandlerException) {
            log.error("Send Message Exception Handler threw an exception", sendMessageExceptionHandlerException);
        }
    }

    private void messageAbortedCallback(T abortedMessage) {
        try {
            sendMessageAbortedHandler.accept(abortedMessage);
        } catch (Exception messageAbortedHandlerException) {
            log.error("Message Aborted Handler threw an exception", messageAbortedHandlerException);
        }
    }

    private boolean isTransactionOpen() {
        return Optional.ofNullable(isTransacted.get()).orElse(false);
    }

    private boolean hasTransactionFailed() {
        return Optional.ofNullable(transactionAlreadyFailed.get()).orElse(false);
    }

    private void clearThreadLocals() {
        Optional.ofNullable(transactionProducer.get()).ifPresent(JMSProducer::close);
        clearThreadLocalsNoClose();
    }

    private void clearThreadLocalsNoClose() {
        transactionProducer.remove();
        isTransacted.remove();
        transactedMessages.remove();
        transactionAlreadyFailed.remove();
    }
}
