package io.github.fishthefirst;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.fishthefirst.data.MessageWithMetadata;
import io.github.fishthefirst.jackson.MessageWithMetadataJacksonMixInConfigurator;
import io.github.fishthefirst.jms.JMSConnectionContextHolder;
import io.github.fishthefirst.jms.JMSConsumer;
import io.github.fishthefirst.jms.JMSContextAwareComponentFactory;
import io.github.fishthefirst.jms.JMSProducerTransactionManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.List;

import static org.apache.activemq.ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
@Slf4j
public class Main {
    static JMSConsumer consumer;

    public static void main(String[] args) throws InterruptedException {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("admin", "admin", "tcp://localhost:62616");
        JMSConnectionContextHolder contextHolder = JMSContextAwareComponentFactory.createContextHolder(activeMQConnectionFactory, INDIVIDUAL_ACKNOWLEDGE);
        contextHolder.setClientId("clientId");
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        MessageWithMetadataJacksonMixInConfigurator.configureMixIn(objectMapper);

        int id = 0;
        //sendMessage(contextHolder, objectMapper, new Message("Hello!", ++id));
        JMSProducerTransactionManager transactionManager = new JMSProducerTransactionManager(contextHolder, (object -> safeObjectMapperWriteValueAsString(object, objectMapper)), Main::sendMessageFailedExceptionHandler, "queue2", false);
        transactionManager.startTransaction();
        for(int i = 0; 100000 > i; i++) {
            if(i % 1000 == 0) {
                log.info("Messages sent {}", i);
            }
            transactionManager.sendObject(new Message("Hello!", ++id));
            //sendMessagesTransacted(contextHolder, objectMapper, List.of(new Message("Hello!", ++id), new Message("Hello!", ++id), new Message("Hello!", ++id)));
        }
        transactionManager.commit();
        transactionManager.close();

        startListening(contextHolder, objectMapper);
        Thread.sleep(60000);
        consumer.close();
        contextHolder.close();
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Data
    public static class Message {
        private String messageString;
        private int id;

        public String toString() {
            return messageString + id;
        }
    }

    static int messagesRecv = 0;

    public static void acceptMessage(MessageWithMetadata messageWithString) {
        messagesRecv++;
        if(messagesRecv % 1000 == 0) {
            log.info("Messages recv {}", messagesRecv);
        }
        //Object payload = messageWithString.getPayload();
        //Class<?> aClass = payload.getClass();
        //log.info("Received message of type {}", aClass.getName());
    }

    public static void startListening(JMSConnectionContextHolder contextHolder, ObjectMapper objectMapper) {
        consumer = JMSContextAwareComponentFactory.createConsumer(contextHolder, Main::acceptMessage, (s) -> {
            try {
                return objectMapper.readValue(s, MessageWithMetadata.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }, "queue2", false, "Consumer1");
        consumer.registerOnReadFailEventHandler(() -> {log.error("Read fail");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        consumer.registerOnUnmarshallFailEventHandler((s) -> log.error("Failed to unmarshall {}", s));
        consumer.registerOnReadTimeoutEventHandler(() -> log.info("Read timeout"));
        consumer.start();
    }

    public static void sendMessage(JMSConnectionContextHolder contextHolder, ObjectMapper objectMapper, Object message) {
        JMSProducerTransactionManager transactionManager = new JMSProducerTransactionManager(contextHolder, (object -> safeObjectMapperWriteValueAsString(object, objectMapper)), Main::sendMessageFailedExceptionHandler, "queue2", false);
        transactionManager.sendObject(message);
        transactionManager.close();
    }

    public static void sendMessagesTransacted(JMSConnectionContextHolder contextHolder, ObjectMapper objectMapper, List<Object> messages) {
        JMSProducerTransactionManager transactionManager = new JMSProducerTransactionManager(contextHolder, (object -> safeObjectMapperWriteValueAsString(object, objectMapper)), Main::sendMessageFailedExceptionHandler, "queue2", false);
        transactionManager.sendObjectsTransacted(messages);
        transactionManager.close();
    }

    public static String safeObjectMapperWriteValueAsString(MessageWithMetadata messageWithMetadata, ObjectMapper objectMapper) {
        try {
            return objectMapper.writeValueAsString(messageWithMetadata);
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException("Exception while serializing object with ObjectMapper", jsonProcessingException);
        }
    }

    public static void sendMessageFailedExceptionHandler(Object json) {
        log.error("Failed to send message {}", json);
    }
}
