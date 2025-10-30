package io.github.fishthefirst.jms;

import io.github.fishthefirst.data.MessageWithMetadata;
import io.github.fishthefirst.handlers.ConsumerStringEventHandler;
import io.github.fishthefirst.handlers.ConsumerVoidEventHandler;
import io.github.fishthefirst.handlers.MessageCallback;
import io.github.fishthefirst.serde.StringToMessageUnmarshaller;
import io.github.fishthefirst.utils.CustomizableThreadFactory;
import io.github.fishthefirst.utils.WatchdogTimer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.fishthefirst.utils.JMSRuntimeExceptionUtils.tryAndLogError;

@Slf4j
public final class JMSConsumer implements AutoCloseable {

    // Atomic refs (w/ Setters)
    private final AtomicReference<ConsumerStringEventHandler> onUnmarshallFailEventHandler = new AtomicReference<>();
    private final AtomicReference<ConsumerVoidEventHandler> onReadFailEventHandler = new AtomicReference<>();
    private final AtomicReference<ConsumerVoidEventHandler> onReadTimeoutEventHandler = new AtomicReference<>();

    // Atomic refs
    private final AtomicBoolean running = new AtomicBoolean();

    // Init/Watchdog
    private final WatchdogTimer watchdogTimer = new WatchdogTimer(this::onReadTimeout);
    private final ScheduledExecutorService clientCreator = Executors.newSingleThreadScheduledExecutor(CustomizableThreadFactory.getInstance(this.getClass().getSimpleName()));

    // Constructor vars
    private final JMSSessionContextSupplier contextProvider;
    private final MessageCallback messageCallback;
    private final StringToMessageUnmarshaller stringToMessageUnmarshaller;
    private final String destinationName;
    private final boolean topic;

    // JMS
    private JMSContext context;
    private jakarta.jms.JMSConsumer consumer;

    // User props
    private String selector;
    private String consumerName;
    private boolean noLocal;

    JMSConsumer(JMSSessionContextSupplier contextProvider, MessageCallback messageCallback, StringToMessageUnmarshaller stringToMessageUnmarshaller, String destinationName, boolean topic, String consumerName) {
        this(contextProvider, messageCallback, stringToMessageUnmarshaller, destinationName, topic, null, consumerName);
    }

    JMSConsumer(JMSSessionContextSupplier contextProvider, MessageCallback messageCallback, StringToMessageUnmarshaller stringToMessageUnmarshaller, String destinationName, boolean topic, String selector, String consumerName) {
        Objects.requireNonNull(contextProvider, "Context provider cannot be null");
        Objects.requireNonNull(messageCallback, "Message callback cannot be null");
        Objects.requireNonNull(stringToMessageUnmarshaller, "Unmarshaller cannot be null");
        Objects.requireNonNull(destinationName, "Destination name cannot be null");
        if (topic && Strings.isBlank(consumerName)) {
            throw new IllegalArgumentException("Consumer name cannot be null or empty for topic consumers");
        }
        this.contextProvider = contextProvider;
        this.messageCallback = messageCallback;
        this.stringToMessageUnmarshaller = stringToMessageUnmarshaller;
        this.selector = selector;
        this.destinationName = destinationName;
        this.topic = topic;
        this.consumerName = consumerName;
    }

    // Register event handlers
    public void registerOnReadFailEventHandler(ConsumerVoidEventHandler eventHandler) {
        Objects.requireNonNull(eventHandler, "Supplied event handler cannot be null");
        onReadFailEventHandler.set(eventHandler);
    }

    public void registerOnReadTimeoutEventHandler(ConsumerVoidEventHandler eventHandler) {
        Objects.requireNonNull(eventHandler, "Supplied event handler cannot be null");
        onReadTimeoutEventHandler.set(eventHandler);
    }

    public void registerOnUnmarshallFailEventHandler(ConsumerStringEventHandler eventHandler) {
        Objects.requireNonNull(eventHandler, "Supplied event handler cannot be null");
        onUnmarshallFailEventHandler.set(eventHandler);
    }

    // User prop setters
    public synchronized void setSelector(String selector) {
        this.selector = selector;
        if (running.get()) {
            closeConsumer();
            doStart();
        }
    }

    public synchronized void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
        if (running.get()) {
            closeConsumer();
            doStart();
        }
    }

    public synchronized void setSelectorAndConsumerName(String selector, String consumerName) {
        this.selector = selector;
        this.consumerName = consumerName;
        if (running.get()) {
            closeConsumer();
            doStart();
        }
    }

    public synchronized void setNoLocal(boolean noLocal) {
        this.noLocal = noLocal;
        if (running.get()) {
            closeConsumer();
            doStart();
        }
    }

    // Consumer controls (affect running status flag)
    public synchronized void start() {
        running.set(true);
        doStart();
    }

    public synchronized void stop() {
        running.set(false);
        doStop();
    }

    @Override
    public synchronized void close() {
        stop();
        if (Objects.nonNull(context)) {
            tryAndLogError(context::close, "An exception was thrown while closing discarded context");
        }
        context = null;
        consumer = null;
        watchdogTimer.close();
        clientCreator.shutdownNow();
    }

    // Private controls (do not affect running status flag)
    private synchronized void doStop() {
        watchdogTimer.stop();
        if (Objects.nonNull(context)) {
            tryAndLogError(context::stop, "An exception was thrown while closing discarded context");
        }
    }

    private synchronized void doClose() {
        doStop();
        if (Objects.nonNull(context)) {
            tryAndLogError(context::close, "An exception was thrown while closing discarded context");
        }
        context = null;
        consumer = null;
    }

    private synchronized void doStart() {
        if (running.get()) {
            if (Objects.isNull(consumer)) {
                clientCreator.schedule(this::tryCreateConsumerLoop, 1000, TimeUnit.MILLISECONDS);
            } else if (Objects.nonNull(context)) {
                tryAndLogError(context::start, "", () -> {
                    doClose();
                    doStart();
                });
            }
        }
    }

    // Events
    private void onException(JMSException exception) {
        doClose();
        doStart();
    }

    private void onReadFail() {
        Optional.ofNullable(onReadFailEventHandler.get()).ifPresent(this::tryCatch);
    }

    private void onReadTimeout() {
        Optional.ofNullable(onReadTimeoutEventHandler.get()).ifPresent(this::tryCatch);
        watchdogTimer.reset();
    }

    private void onUnmarshallFail(String string) {
        Optional.ofNullable(onUnmarshallFailEventHandler.get()).ifPresent(c -> tryCatch(string, c));
    }

    // Create JMS components
    private void createContext() {
        JMSContextWrapper contextWrapper = Objects.requireNonNull(contextProvider.createContext(this::onException), "Context provider returned null. Exception thrown to bail out.");
        context = contextWrapper.getContext();
    }

    private synchronized void createConsumer() {
        doClose();
        if (Objects.isNull(context)) {
            createContext();
        }
        if (!topic) {
            if (Objects.nonNull(selector)) {
                log.warn("Selector usage for queue is not recommended");
            }
            Queue destination = context.createQueue(destinationName);
            consumer = context.createConsumer(destination, selector, noLocal);
        } else {
            Topic destination = context.createTopic(destinationName);
            consumer = context.createDurableConsumer(destination, consumerName, selector, noLocal);
        }
        consumer.setMessageListener(this::handleMessage);
        watchdogTimer.start(10000);
        log.info("Consumer {} started on destination: {}", consumerName, destinationName);
    }

    private void tryCreateConsumerLoop() {
        if (Objects.isNull(consumer) && running.get()) {
            tryAndLogError(this::createConsumer, "Exception thrown when creating consumer", () -> {
                doClose();
                clientCreator.schedule(this::tryCreateConsumerLoop, 1000, TimeUnit.MILLISECONDS);
            });
        }
    }

    // Close components
    private synchronized void closeConsumer() {
        doStop();
        if (Objects.nonNull(consumer)) {
            consumer.close();
            consumer = null;
        }
    }

    // Message processing
    private void tryCatch(ConsumerVoidEventHandler eventHandler) {
        try {
            eventHandler.run();
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private void tryCatch(String s, ConsumerStringEventHandler eventHandler) {
        try {
            eventHandler.accept(s);
        } catch (Exception e) {
            log.error("", e);
        }
    }

    private MessageWithMetadata unmarshall(String s) {
        try {
            return stringToMessageUnmarshaller.apply(s);
        } catch (Exception e) {
            onUnmarshallFail(s);
            throw new RuntimeException(e);
        }
    }

    private String parse(TextMessage textMessage) {
        try {
            return textMessage.getText();
        } catch (JMSException e) {
            onReadFail();
            throw new RuntimeException("Failed to parse JMS Message!", e.getCause());
        }
    }

    private synchronized void handleMessage(Message message) {
        watchdogTimer.stop();
        String string = parse((TextMessage) message);
        MessageWithMetadata unmarshall = unmarshall(string);
        try {
            messageCallback.accept(unmarshall);
            commitOrAck(message);
        } catch (Exception e) {
            log.error("Unhandled exception from consumer callback", e);
        }
        watchdogTimer.start(10000);
    }

    private void commitOrAck(Message message) {
        try {
            if (context.getTransacted()) {
                context.commit();
            } else {
                message.acknowledge();
            }
        } catch (Exception e) {
            log.error("An exception was thrown while commiting/acknowledging", e);
        }
    }
}