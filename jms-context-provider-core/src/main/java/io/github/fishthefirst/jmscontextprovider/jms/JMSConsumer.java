package io.github.fishthefirst.jmscontextprovider.jms;

import io.github.fishthefirst.jmscontextprovider.enums.JMSConsumerBehaviour;
import io.github.fishthefirst.jmscontextprovider.utils.CustomizableThreadFactory;
import io.github.fishthefirst.jmscontextprovider.utils.JMSRuntimeExceptionUtils;
import io.github.fishthefirst.jmscontextprovider.utils.WatchdogTimer;
import io.github.fishthefirst.jmscontextprovider.handlers.ConsumerStringEventHandler;
import io.github.fishthefirst.jmscontextprovider.handlers.ConsumerVoidEventHandler;
import io.github.fishthefirst.jmscontextprovider.handlers.MessageCallback;
import io.github.fishthefirst.jmscontextprovider.serde.StringToObjectUnmarshaller;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.github.fishthefirst.jmscontextprovider.enums.JMSConsumerBehaviour.DISCARD;
import static io.github.fishthefirst.jmscontextprovider.enums.JMSConsumerBehaviour.DISCARD_AFTER_RETRY_COUNT_EXCEEDED;
import static io.github.fishthefirst.jmscontextprovider.utils.JMSRuntimeExceptionUtils.tryAndLogError;

public final class JMSConsumer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(JMSConsumer.class);

    // Atomic refs (w/ Setters)
    private final AtomicReference<ConsumerStringEventHandler> onUnmarshallFailEventHandler = new AtomicReference<>();
    private final AtomicReference<ConsumerVoidEventHandler> onReadFailEventHandler = new AtomicReference<>();
    private final AtomicReference<ConsumerVoidEventHandler> onReadTimeoutEventHandler = new AtomicReference<>();

    // Atomic refs
    private final AtomicBoolean running = new AtomicBoolean();

    // Init/Watchdog
    private final WatchdogTimer watchdogTimer = new WatchdogTimer(this::onReadTimeout);
    private final ScheduledExecutorService clientCreator = Executors.newSingleThreadScheduledExecutor(CustomizableThreadFactory.getInstance(this));

    // Constructor vars
    private final JMSSessionContextSupplier contextProvider;
    private final MessageCallback messageCallback;
    private final StringToObjectUnmarshaller stringToObjectUnmarshaller;
    private final String destinationName;
    private final boolean topic;

    // JMS
    private JMSContext context;
    private jakarta.jms.JMSConsumer consumer;
    private int unmarshalTryCount;
    private int unmarshalRetryLimit;
    private int consumeRetryLimit;
    private int consumeTryCount;
    private String lastParsedJMSMessageId;

    // User props
    private String selector;
    private String consumerName;
    private boolean noLocal;
    private JMSConsumerBehaviour onParseFailBehaviour = JMSConsumerBehaviour.ROLLBACK;
    private JMSConsumerBehaviour onUnmarshallFailBehaviour = JMSConsumerBehaviour.ROLLBACK;
    private JMSConsumerBehaviour onConsumeFailBehaviour = JMSConsumerBehaviour.ROLLBACK;

    JMSConsumer(JMSSessionContextSupplier contextProvider,
                MessageCallback messageCallback,
                StringToObjectUnmarshaller stringToObjectUnmarshaller,
                String destinationName,
                boolean topic,
                String consumerName) {
        this(contextProvider, messageCallback, stringToObjectUnmarshaller, destinationName, topic, null, consumerName);
    }

    JMSConsumer(JMSSessionContextSupplier contextProvider,
                MessageCallback messageCallback,
                StringToObjectUnmarshaller stringToObjectUnmarshaller,
                String destinationName,
                boolean topic,
                String selector,
                String consumerName) {
        Objects.requireNonNull(contextProvider, "Context provider cannot be null");
        Objects.requireNonNull(messageCallback, "Message callback cannot be null");
        Objects.requireNonNull(stringToObjectUnmarshaller, "Unmarshaller cannot be null");
        Objects.requireNonNull(destinationName, "Destination name cannot be null");
        if (topic && consumerName.isBlank()) {
            throw new IllegalArgumentException("Consumer name cannot be null or empty for topic consumers");
        }
        this.contextProvider = contextProvider;
        this.messageCallback = messageCallback;
        this.stringToObjectUnmarshaller = stringToObjectUnmarshaller;
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

    public void setOnUnmarshallFailBehaviour(JMSConsumerBehaviour behaviour) {
        if (behaviour == DISCARD_AFTER_RETRY_COUNT_EXCEEDED) behaviour = DISCARD;

        this.onUnmarshallFailBehaviour = behaviour;
    }

    /**
     * Setting this limit will automatically set the onUnmarshallFailBehaviour to DISCARD_AFTER_RETRY_COUNT_EXCEEDED
     * if retryLimit is greater than 0, or DISCARD otherwise.
     *
     * @param retryLimit The maximum number of retries
     */
    public void setUnmarshalRetryLimit(int retryLimit) {
        if (retryLimit < 1) {
            this.onUnmarshallFailBehaviour = DISCARD;
        } else {
            this.onUnmarshallFailBehaviour = DISCARD_AFTER_RETRY_COUNT_EXCEEDED;
            this.unmarshalRetryLimit = retryLimit;
        }
    }

    public void setOnParseFailBehaviour(JMSConsumerBehaviour behaviour) {
        this.onParseFailBehaviour = behaviour;
    }

    public void setOnConsumeFailBehaviour(JMSConsumerBehaviour behaviour) {
        if (behaviour == DISCARD_AFTER_RETRY_COUNT_EXCEEDED) behaviour = DISCARD;

        this.onConsumeFailBehaviour = behaviour;
    }

    /**
     * Setting this limit will automatically set the onConsumeFailBehaviour to DISCARD_AFTER_RETRY_COUNT_EXCEEDED
     * if retryLimit is greater than 0, or DISCARD otherwise.
     *
     * @param retryLimit The maximum number of retries
     */
    public void setConsumeRetryLimit(int retryLimit) {
        if (retryLimit < 1) {
            this.onConsumeFailBehaviour = DISCARD;
        } else {
            this.onConsumeFailBehaviour = DISCARD_AFTER_RETRY_COUNT_EXCEEDED;
            this.consumeRetryLimit = retryLimit;
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
        Optional.ofNullable(onReadFailEventHandler.get()).ifPresent(JMSRuntimeExceptionUtils::tryAndLogError);
    }

    private void onReadTimeout() {
        Optional.ofNullable(onReadTimeoutEventHandler.get()).ifPresent(JMSRuntimeExceptionUtils::tryAndLogError);
        watchdogTimer.reset();
    }

    private void onUnmarshallFail(String string) {
        Optional.ofNullable(onUnmarshallFailEventHandler.get()).ifPresent(handler -> tryAndLogError(string, handler));
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
    private Object unmarshall(String s) {
        try {
            return stringToObjectUnmarshaller.unmarshal(s);
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

        try {
            String string = getStringFromMessage(message);
            if(Objects.isNull(string)) return;

            handleJmsMessageIdAndTryCount(message);

            Object unmarshalledObject = tryUnmarshall(message, string);
            if(Objects.isNull(unmarshalledObject)) return;

            invokeCallback(message, unmarshalledObject);
        } finally {
            watchdogTimer.start(10000);
        }
    }

    private void invokeCallback(Message message, Object unmarshalledObject) {
        try {
            messageCallback.accept(unmarshalledObject);
            consumeTryCount = 0;
            ackAndCommit(message);
        } catch (Exception e) {
            consumeTryCount++;
            switch (onConsumeFailBehaviour) {
                case DISCARD -> {
                    log.warn("Discarding unprocessed object with ID {} due to an unhandled exception from consumer callback", lastParsedJMSMessageId, e);
                    ackAndCommit(message);
                }
                case DISCARD_AFTER_RETRY_COUNT_EXCEEDED -> {
                    if (consumeTryCount > consumeRetryLimit) {
                        log.warn("Discarding unprocessed object with ID {} due to an unhandled exception from consumer callback and exceeding the ConsumeRetryLimit", lastParsedJMSMessageId, e);
                        ackAndCommit(message);
                    } else {
                        log.error("Unhandled exception from consumer callback while consuming object with ID {}", lastParsedJMSMessageId, e);
                    }
                }
                case ROLLBACK -> log.error("Unhandled exception from consumer callback while consuming object with ID {}", lastParsedJMSMessageId, e);
            }

        }
    }

    private void handleJmsMessageIdAndTryCount(Message message) {
        String jmsMessageId;
        try {
            jmsMessageId = message.getJMSMessageID();
            if (!Objects.equals(lastParsedJMSMessageId, jmsMessageId)) {
                unmarshalTryCount = 0;
                consumeTryCount = 0;
            }
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
        lastParsedJMSMessageId = jmsMessageId;
    }

    private String getStringFromMessage(Message message) {
        String string = null;
        try {
            string = parse((TextMessage) message);
        } catch (Exception e) {
            handleParseFailure(message, e);
        }
        return string;
    }

    private void ackAndCommit(Message message) {
        try {
            message.acknowledge();
            if (context.getTransacted()) {
                context.commit();
            }
        } catch (Exception e) {
            log.error("An exception was thrown while commiting/acknowledging message with ID {}", lastParsedJMSMessageId, e);
        }
    }

    private Object tryUnmarshall(Message message, String string) {
        Object unmarshalledObject = null;
        try {
            unmarshalledObject = unmarshall(string);
            unmarshalTryCount = 0;
        } catch (Exception e) {
            unmarshalTryCount++;
            switch (onUnmarshallFailBehaviour) {
                case DISCARD -> {
                    log.warn("Discarding unmarshallable object with ID {} due to", lastParsedJMSMessageId, e);
                    ackAndCommit(message);
                }
                case DISCARD_AFTER_RETRY_COUNT_EXCEEDED -> {
                    if (unmarshalTryCount > unmarshalRetryLimit) {
                        log.warn("Discarding unmarshallable object with ID {} due to an exception and exceeding UnmarshalRetryLimit", lastParsedJMSMessageId, e);
                        ackAndCommit(message);
                    } else {
                        log.error("An exception was thrown while unmarshalling object with ID {}", lastParsedJMSMessageId, e);
                    }
                }
                case ROLLBACK -> log.error("An exception was thrown while unmarshalling object with ID {}", lastParsedJMSMessageId, e);
            }
        }
        return unmarshalledObject;
    }

    private void handleParseFailure(Message message, Exception e) {
        switch (onParseFailBehaviour) {
            case DISCARD -> {
                log.warn("Discarding unparseable object with ID {} due to", lastParsedJMSMessageId, e);
                ackAndCommit(message);
            }
            case ROLLBACK -> log.error("Failed to parse message with ID {} due to", lastParsedJMSMessageId, e);
        }
    }
}