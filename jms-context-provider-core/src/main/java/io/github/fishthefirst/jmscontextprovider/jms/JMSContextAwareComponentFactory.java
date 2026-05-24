package io.github.fishthefirst.jmscontextprovider.jms;


import io.github.fishthefirst.jmscontextprovider.handlers.MessageCallback;
import io.github.fishthefirst.jmscontextprovider.handlers.SendMessageAbortedHandler;
import io.github.fishthefirst.jmscontextprovider.handlers.SendMessageExceptionHandler;
import io.github.fishthefirst.jmscontextprovider.serde.MessageProcessor;
import io.github.fishthefirst.jmscontextprovider.serde.ObjectToStringMarshaller;
import io.github.fishthefirst.jmscontextprovider.serde.StringToObjectUnmarshaller;
import jakarta.jms.ConnectionFactory;

public final class JMSContextAwareComponentFactory {

    private JMSContextAwareComponentFactory() {
    }

    public static JMSConnectionContextHolder createContextHolder(ConnectionFactory connectionFactory) {
        JMSConnectionContextHolder jmsConnectionContextHolder = new JMSConnectionContextHolder(connectionFactory);
        jmsConnectionContextHolder.setAllowContextWithoutClientId(false);
        return jmsConnectionContextHolder;
    }

    public static <T> JMSConsumer<T> createConsumer(JMSConnectionContextHolder mainContextHolder,
                                                    MessageCallback<T>  messageCallback,
                                                    StringToObjectUnmarshaller<T>  stringToObjectUnmarshaller,
                                                    String destinationName,
                                                    boolean topic,
                                                    String consumerName,
                                                    int sessionMode) {
        return new JMSConsumer<>(
                new JMSSessionContextSupplier(mainContextHolder, sessionMode),
                messageCallback,
                stringToObjectUnmarshaller,
                destinationName,
                topic,
                consumerName);
    }

    public <T> JMSProducerTransactionManager<T> createTransactionManager(JMSConnectionContextHolder connectionContextHolder,
                                                                         ObjectToStringMarshaller<T> messageToStringMarshaller,
                                                                         SendMessageExceptionHandler<T> sendMessageExceptionHandler,
                                                                         SendMessageAbortedHandler<T> sendMessageAbortedHandler,
                                                                         MessageProcessor<T> messagePreProcessor,
                                                                         MessageProcessor<T> messagePostProcessor,
                                                                         String destinationName,
                                                                         boolean topic) {
        return new JMSProducerTransactionManager<>(
                connectionContextHolder,
                messageToStringMarshaller,
                sendMessageExceptionHandler,
                sendMessageAbortedHandler,
                messagePreProcessor,
                messagePostProcessor,
                destinationName,
                topic);
    }

    public static <T> JMSProducer<T> createProducer(JMSConnectionContextHolder mainContextHolder,
                                                    ObjectToStringMarshaller<T> messageToStringMarshaller,
                                                    MessageProcessor<T> messagePreProcessor,
                                                    MessageProcessor<T> messagePostProcessor,
                                                    String destinationName,
                                                    boolean topic,
                                                    String producerName,
                                                    int sessionMode,
                                                    boolean keepAlive) {
        return new JMSProducer<>(
                new JMSSessionContextSupplier(mainContextHolder, sessionMode),
                messageToStringMarshaller,
                messagePreProcessor,
                messagePostProcessor,
                destinationName,
                topic,
                producerName,
                7*60*60*24,
                keepAlive);
    }
}