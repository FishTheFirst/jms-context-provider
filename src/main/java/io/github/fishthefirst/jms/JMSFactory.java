package io.github.fishthefirst.jms;

import io.github.fishthefirst.handlers.MessageCallback;
import io.github.fishthefirst.serde.StringToMessageUnmarshaller;
import jakarta.jms.ConnectionFactory;

public final class JMSFactory {

    private JMSFactory(){}

    public static JMSMainContextHolder createContextHolder(ConnectionFactory connectionFactory, int sessionMode) {
        return new JMSMainContextHolder(connectionFactory, sessionMode);
    }

    public static JMSConsumerHolder createConsumer(JMSMainContextHolder mainContextHolder,
                                                   MessageCallback messageCallback,
                                                   StringToMessageUnmarshaller stringToMessageUnmarshaller,
                                                   String destinationName,
                                                   boolean topic,
                                                   String consumerName) {
        return new JMSConsumerHolder(new JMSSecondaryContextHolder(mainContextHolder, mainContextHolder.getSessionMode())::createContext, messageCallback, stringToMessageUnmarshaller, destinationName, topic, consumerName);
    }
}