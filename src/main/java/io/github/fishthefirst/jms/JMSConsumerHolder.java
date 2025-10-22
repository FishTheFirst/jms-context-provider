package io.github.fishthefirst.jms;

import io.github.fishthefirst.data.MessageWithMetadata;
import io.github.fishthefirst.contextproviders.FixedSessionModeJMSContextProvider;
import io.github.fishthefirst.handlers.ConsumerStringEventHandler;
import io.github.fishthefirst.handlers.ConsumerVoidEventHandler;
import io.github.fishthefirst.handlers.MessageCallback;
import io.github.fishthefirst.serde.StringToMessageUnmarshaller;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.apache.logging.log4j.util.Supplier;

import java.io.EOFException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public final class JMSConsumerHolder implements Runnable, AutoCloseable {
    private final FixedSessionModeJMSContextProvider contextProvider;
    private final Set<ConsumerStringEventHandler> onUnmarshallFailEventHandlers = new HashSet<>();
    private final Set<ConsumerVoidEventHandler> onReadFailEventHandlers = new HashSet<>();
    private final Set<ConsumerVoidEventHandler> onReadTimeoutEventHandlers = new HashSet<>();
    private final MessageCallback messageCallback;
    private final StringToMessageUnmarshaller stringToMessageUnmarshaller;
    private final String destinationName;
    private final boolean topic;
    private JMSConsumer consumer;
    private String selector;
    private String consumerName;

    JMSConsumerHolder(FixedSessionModeJMSContextProvider contextProvider, MessageCallback messageCallback, StringToMessageUnmarshaller stringToMessageUnmarshaller, String destinationName, boolean topic, String consumerName) {
        this(contextProvider, messageCallback, stringToMessageUnmarshaller, destinationName, topic, null, consumerName);
    }

    JMSConsumerHolder(FixedSessionModeJMSContextProvider contextProvider, MessageCallback messageCallback, StringToMessageUnmarshaller stringToMessageUnmarshaller, String destinationName, boolean topic, String selector, String consumerName) {
        Objects.requireNonNull(contextProvider, "Context provider cannot be null");
        Objects.requireNonNull(messageCallback, "Message callback cannot be null");
        Objects.requireNonNull(stringToMessageUnmarshaller, "Unmarshaller cannot be null");
        Objects.requireNonNull(destinationName, "Destination name cannot be null");
        if (topic && Strings.isBlank(consumerName))
            throw new IllegalArgumentException("Consumer name cannot be null or empty for topic consumers");
        this.contextProvider = contextProvider;
        this.messageCallback = messageCallback;
        this.stringToMessageUnmarshaller = stringToMessageUnmarshaller;
        this.selector = selector;
        this.destinationName = destinationName;
        this.topic = topic;
        this.consumerName = consumerName;
        //tryCreateConsumer();
    }

    public void setSelector(String selector) {
        this.selector = selector;
        if (Objects.nonNull(consumer))
            tryCreateConsumer();
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
        if (Objects.nonNull(consumer))
            tryCreateConsumer();
    }

    public void setSelectorAndConsumerName(String selector, String consumerName) {
        this.selector = selector;
        this.consumerName = consumerName;
        if (Objects.nonNull(consumer))
            tryCreateConsumer();
    }

    @Override
    public void close() {
        if (Objects.nonNull(consumer)) {
            consumer.close();
            consumer = null;
            log.info("Consumer {} closed", consumerName);
        }
    }

    public void registerOnReadFailEventHandler(ConsumerVoidEventHandler eventHandler) {
        onReadFailEventHandlers.add(eventHandler);
    }

    public void registerOnReadTimeoutEventHandler(ConsumerVoidEventHandler eventHandler) {
        onReadTimeoutEventHandlers.add(eventHandler);
    }

    public void registerOnUnmarshallFailEventHandler(ConsumerStringEventHandler eventHandler) {
        onUnmarshallFailEventHandlers.add(eventHandler);
    }

    private void onReadFail() {
        onReadFailEventHandlers.forEach(this::tryCatch);
    }

    private void onReadTimeout() {
        onReadTimeoutEventHandlers.forEach(this::tryCatch);
    }

    private void onUnmarshallFail(String string) {
        onUnmarshallFailEventHandlers.forEach(c -> tryCatch(string, c));
    }

    private void createConsumer(JMSContext context) {
        if (Objects.nonNull(consumer)) close();
        context = Objects.requireNonNullElse(context, Objects.requireNonNull(contextProvider.get(), "Context provider returned null. Exception thrown to bail out."));
        if (!topic) {
            if (Objects.nonNull(selector))
                log.warn("Selector usage for queue is not recommended");
            Queue destination = context.createQueue(destinationName);
            consumer = context.createConsumer(destination, selector);
        } else {
            Topic destination = context.createTopic(destinationName);
            consumer = context.createDurableConsumer(destination, consumerName, selector, false);
        }
    }

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

    private void tryCreateConsumer() {
        try {
            createConsumer(contextProvider.get());
        } catch (Exception e) {
            close();
            //log.error("", e);
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
            throw new RuntimeException("Failed to parse JMS Message!", e.getCause());
        }
    }

    private void handleMessage(TextMessage message) {
        String string = parse(message);
        MessageWithMetadata unmarshall = unmarshall(string);
        try {
            messageCallback.accept(unmarshall);
            message.acknowledge();
        } catch (Exception e) {
            log.error("Unhandled exception from consumer callback", e);
        }
    }

    private static void doIf(boolean b, Runnable t, Runnable f) {
        if (b) {
            t.run();
        } else {
            f.run();
        }
    }

    @Override
    public synchronized void run() {
        JMSContext context = null;
        try {
            context = contextProvider.get();
            log.info("Consumer {} run", consumerName);
            if (Objects.isNull(consumer)) {
                createConsumer(context);
            }
            Optional.ofNullable((TextMessage) consumer.receive(10000L))
                    .ifPresentOrElse(this::handleMessage, this::onReadTimeout);
            if (context.getTransacted()) {
                context.commit();
            }
        } catch (JMSRuntimeException exception) {
            Throwable realException = exception.getCause();
            if (realException instanceof JMSException jmse) {
                realException = jmse.getLinkedException();
            }
            if (realException instanceof InterruptedException ie) {
                log.warn("Consumer {} exited from InterruptException", consumerName);
                throw new RuntimeException(ie);
            }
            if (realException instanceof EOFException eof) {
                tryCreateConsumer();
            }
            log.error("Consumer {} exception", consumerName);
            log.error("", realException);
            onReadFail();
            Optional.ofNullable(context).ifPresent(c -> {
                try {
                    doIf(c.getTransacted(), c::recover, c::rollback);
                } catch (JMSRuntimeException e) {
                    log.error("Consumer {} exception", consumerName, e);
                }
            });
        } catch (Exception e) {
            log.error("Consumer {} exception", consumerName, e);
        }
    }
}