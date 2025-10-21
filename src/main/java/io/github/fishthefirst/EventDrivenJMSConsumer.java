package io.github.fishthefirst;

import jakarta.jms.ExceptionListener;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import lombok.extern.slf4j.Slf4j;

import java.io.EOFException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Slf4j
public class EventDrivenJMSConsumer implements Runnable, ExceptionListener, AutoCloseable {
        private final JMSContext context;
        private final Set<ConsumerStringEventHandler> onUnmarshallFailEventHandlers = new HashSet<>();
        private final Set<ConsumerVoidEventHandler> onReadFailEventHandlers = new HashSet<>();
        private final MessageCallback messageCallback;
        private final StringToMessageUnmarshaller stringToMessageUnmarshaller;
        private final String destinationName;
        private final boolean topic;
        private JMSConsumer consumer;
        private String selector;
        private String consumerName;

        public EventDrivenJMSConsumer(JMSContext context, MessageCallback messageCallback, StringToMessageUnmarshaller stringToMessageUnmarshaller, String destinationName, boolean topic, String consumerName) {
            this(context, messageCallback, stringToMessageUnmarshaller, destinationName, topic, null, consumerName);
        }

        public EventDrivenJMSConsumer(JMSContext context, MessageCallback messageCallback, StringToMessageUnmarshaller stringToMessageUnmarshaller, String destinationName, boolean topic, String selector, String consumerName) {
            this.context = context;
            this.messageCallback = messageCallback;
            this.stringToMessageUnmarshaller = stringToMessageUnmarshaller;
            this.selector = selector;
            this.destinationName = destinationName;
            this.topic = topic;
            this.consumerName = consumerName;
            createConsumer();
        }

        public void setSelector(String selector) {
            this.selector = selector;
            createConsumer();
        }

        public void setConsumerName(String consumerName) {
            this.consumerName = consumerName;
            createConsumer();
        }

        public void setSelectorAndConsumerName(String selector, String consumerName) {
            this.selector = selector;
            this.consumerName = consumerName;
            createConsumer();
        }

        @Override
        public void close() {
            context.stop();
            consumer.close();
            context.close();
            log.info("Consumer {} closed", consumerName);
        }

        @Override
        public void onException(JMSException e) {
            log.error("onException", e);
        }

        public void registerOnReadFailEventHandler(ConsumerVoidEventHandler eventHandler) {
            onReadFailEventHandlers.add(eventHandler);
        }

        public void registerOnUnmarshallFailEventHandler(ConsumerStringEventHandler eventHandler) {
            onUnmarshallFailEventHandlers.add(eventHandler);
        }

        private void onReadFail() {
            onReadFailEventHandlers.forEach(this::tryCatch);
        }

        private void onUnmarshallFail(String string) {
            onUnmarshallFailEventHandlers.forEach(c -> tryCatch(string, c));
        }

        private void createConsumer() {
            if(Objects.nonNull(consumer)) consumer.close();
            synchronized (this) {
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

        private MessageWithMetadata unmarshall(String s) {
            try {
                return stringToMessageUnmarshaller.apply(s);
            }catch (Exception e) {
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
            } catch (Exception e) {
                log.error("Unhandled exception from consumer callback", e);
            }
        }

        @Override
        public synchronized void run() {
            try {
                log.info("Consumer {} run", consumerName);
                Optional.ofNullable((TextMessage) consumer.receive(10000L))
                        .ifPresentOrElse(this::handleMessage, this::onReadFail);
                context.commit();
            } catch (JMSRuntimeException exception) {
                Throwable realException = exception.getCause();
                if(realException instanceof JMSException jmse)
                    realException = jmse.getLinkedException();
                if (realException instanceof InterruptedException ie) {
                    log.warn("Consumer {} exited from InterruptException", consumerName);
                    return;
                }
                if(realException instanceof EOFException eof) {
                    createConsumer();
                }
                log.error("Consumer {} exception", consumerName, realException);
                onReadFail();
                context.recover();
            }
        }
    }