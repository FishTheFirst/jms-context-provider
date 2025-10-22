package io.github.fishthefirst.jms;

import io.github.fishthefirst.handlers.MessageCallback;
import io.github.fishthefirst.serde.StringToMessageUnmarshaller;
import jakarta.jms.ConnectionFactory;

public class JMSFactory {
    public static JMSMainContextHolder createContextHolder(ConnectionFactory connectionFactory, int sessionMode) {
        return new JMSMainContextHolder(connectionFactory, sessionMode);
    }

    public static JMSConsumerHolder createConsumer(JMSMainContextHolder connectionFactory,
                                                   MessageCallback messageCallback,
                                                   StringToMessageUnmarshaller stringToMessageUnmarshaller,
                                                   String destinationName,
                                                   boolean topic,
                                                   String consumerName) {
        return new JMSConsumerHolder(connectionFactory::createContext, messageCallback, stringToMessageUnmarshaller, destinationName, topic, consumerName);
    }
}