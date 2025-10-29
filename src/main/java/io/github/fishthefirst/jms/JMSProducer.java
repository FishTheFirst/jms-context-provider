package io.github.fishthefirst.jms;

import io.github.fishthefirst.serde.MessagePreprocessor;
import io.github.fishthefirst.serde.ObjectToStringMarshaller;
import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.TextMessage;

import java.util.Objects;

public class JMSProducer {
    private final ObjectToStringMarshaller objectToStringMarshaller;
    private final MessagePreprocessor messagePreprocessor;
    private final JMSSessionContextSupplier contextSupplier;
    private final boolean isTopic;
    private final String destinationName;
    private final String producerName;
    private final Integer messageTimeToLive;
    private JMSContext context;
    private jakarta.jms.JMSProducer jmsProducer;
    private Destination destination;

    JMSProducer(JMSSessionContextSupplier contextSupplier,
                ObjectToStringMarshaller objectToStringMarshaller,
                MessagePreprocessor messagePreprocessor,
                String destinationName,
                boolean isTopic,
                String producerName,
                Integer messageTimeToLive) {
        this.objectToStringMarshaller = objectToStringMarshaller;
        this.messagePreprocessor = messagePreprocessor;
        this.contextSupplier = contextSupplier;
        this.isTopic = isTopic;
        this.destinationName = destinationName;
        this.producerName = producerName;
        this.messageTimeToLive = messageTimeToLive;
    }

    public void sendMessage(Object o) {
        String string = objectToStringMarshaller.apply(o);
        sendMessage(string);
    }

    public void sendMessage(String s) {
        TextMessage textMessage = context.createTextMessage(s);
        messagePreprocessor.accept(textMessage);
        jmsProducer.send(destination, textMessage);
    }

    private void createProducer() {
        JMSContextWrapper contextWrapper = contextSupplier.createContext(this::onException);
        this.context = contextWrapper.getContext();
        this.jmsProducer = context.createProducer().setTimeToLive(messageTimeToLive);
        if(isTopic) {
            destination = context.createTopic(destinationName);
        }
        else {
            destination = context.createQueue(destinationName);
        }
    }

    private void onException(JMSException e) {
        if(Objects.nonNull(context))
            context.close();
        context = null;
    }
}
