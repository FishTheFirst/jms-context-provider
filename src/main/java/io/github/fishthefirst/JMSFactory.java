package io.github.fishthefirst;

import jakarta.jms.ConnectionFactory;

public class JMSFactory {
    public static JMSContextProvider createProvider(ConnectionFactory connectionFactory, int sessionMode) {
        return new JMSContextProvider(connectionFactory, sessionMode);
    }

    public static EventDrivenJMSConsumer createConsumer(JMSContextProvider connectionFactory,
                                                        MessageCallback messageCallback,
                                                        StringToMessageUnmarshaller stringToMessageUnmarshaller,
                                                        String destinationName, boolean topic, String consumerName) {
        return new EventDrivenJMSConsumer(connectionFactory.createContext(), messageCallback, stringToMessageUnmarshaller, destinationName, topic, consumerName);
    }
}