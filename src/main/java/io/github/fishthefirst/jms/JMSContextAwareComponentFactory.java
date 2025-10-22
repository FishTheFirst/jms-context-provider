package io.github.fishthefirst.jms;

import io.github.fishthefirst.handlers.MessageCallback;
import io.github.fishthefirst.serde.StringToMessageUnmarshaller;
import jakarta.jms.ConnectionFactory;

public final class JMSContextAwareComponentFactory {

    private JMSContextAwareComponentFactory(){}

    public static JMSConnectionContextHolder createContextHolder(ConnectionFactory connectionFactory, int sessionMode) {
        return new JMSConnectionContextHolder(connectionFactory, sessionMode);
    }

    public static JMSConsumer createConsumer(JMSConnectionContextHolder mainContextHolder,
                                             MessageCallback messageCallback,
                                             StringToMessageUnmarshaller stringToMessageUnmarshaller,
                                             String destinationName,
                                             boolean topic,
                                             String consumerName) {
        return new JMSConsumer(new JMSSessionContextSupplier(mainContextHolder, mainContextHolder.getSessionMode())::createContext, messageCallback, stringToMessageUnmarshaller, destinationName, topic, consumerName);
    }
}