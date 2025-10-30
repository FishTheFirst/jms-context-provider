package io.github.fishthefirst.jms;

import io.github.fishthefirst.handlers.MessageCallback;
import io.github.fishthefirst.serde.ObjectToStringMarshaller;
import io.github.fishthefirst.serde.StringToMessageUnmarshaller;
import jakarta.jms.ConnectionFactory;

public final class JMSContextAwareComponentFactory {

    private JMSContextAwareComponentFactory() {
    }

    public static JMSConnectionContextHolder createContextHolder(ConnectionFactory connectionFactory, int sessionMode) {
        return new JMSConnectionContextHolder(connectionFactory, sessionMode);
    }

    public static JMSConsumer createConsumer(JMSConnectionContextHolder mainContextHolder,
                                             MessageCallback messageCallback,
                                             StringToMessageUnmarshaller stringToMessageUnmarshaller,
                                             String destinationName,
                                             boolean topic,
                                             String consumerName) {
        return new JMSConsumer(new JMSSessionContextSupplier(mainContextHolder, mainContextHolder.getSessionMode()), messageCallback, stringToMessageUnmarshaller, destinationName, topic, consumerName);
    }

    public static JMSConsumer createConsumer(JMSConnectionContextHolder mainContextHolder,
                                             MessageCallback messageCallback,
                                             StringToMessageUnmarshaller stringToMessageUnmarshaller,
                                             String destinationName,
                                             boolean topic,
                                             String consumerName,
                                             int sessionMode) {
        return new JMSConsumer(new JMSSessionContextSupplier(mainContextHolder, sessionMode), messageCallback, stringToMessageUnmarshaller, destinationName, topic, consumerName);
    }

    public static JMSProducer createProducer(JMSConnectionContextHolder mainContextHolder,
                                             ObjectToStringMarshaller objectToStringMarshaller,
                                             String destinationName,
                                             boolean topic,
                                             String producerName,
                                             int sessionMode,
                                             boolean keepAlive) {
        return new JMSProducer(new JMSSessionContextSupplier(mainContextHolder, sessionMode), objectToStringMarshaller, t-> {}, destinationName, topic, producerName, null, keepAlive);
    }
}