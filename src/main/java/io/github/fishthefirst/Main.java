package io.github.fishthefirst;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.fishthefirst.data.MessageWithMetadata;
import io.github.fishthefirst.jackson.MessageWithMetadataJacksonMixInConfigurator;
import io.github.fishthefirst.jms.JMSConnectionContextHolder;
import io.github.fishthefirst.jms.JMSConsumer;
import io.github.fishthefirst.jms.JMSContextAwareComponentFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;

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
        MessageWithMetadataJacksonMixInConfigurator.configureMixIn(objectMapper);
        consumer = JMSContextAwareComponentFactory.createConsumer(contextHolder, Main::acceptMessage, (s) -> {
            try {
                return objectMapper.readValue(s, MessageWithMetadata.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }, "queue", true, "Consumer1");
        consumer.registerOnReadFailEventHandler(() -> {log.error("Read fail");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        consumer.registerOnUnmarshallFailEventHandler((s) -> log.error("Failed to unmarshall {}", s));
        consumer.registerOnReadTimeoutEventHandler(() -> {log.info("Read timeout"); });
        consumer.start();


        JMSConsumer consumer2 = JMSContextAwareComponentFactory.createConsumer(contextHolder, Main::acceptMessage, (s) -> {
            try {
                return objectMapper.readValue(s, MessageWithMetadata.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }, "queue", true, "Consumer2");
        consumer2.registerOnReadFailEventHandler(() -> {log.error("Read fail");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        consumer2.registerOnUnmarshallFailEventHandler((s) -> log.error("Failed to unmarshall {}", s));
        consumer2.registerOnReadTimeoutEventHandler(() -> {log.info("Read timeout"); });

        Thread.sleep(5000);
        for (int j = 0; j < 5; j++) {
            for (int i = 0; i < 10000; i++) {
                contextHolder.setClientId(String.valueOf(i));
            }
            consumer.stop();
        }
        Thread.sleep(60000);


        consumer.close();
        consumer2.close();
        contextHolder.close();
    }

    public static void acceptMessage(MessageWithMetadata messageWithString) {
        Object payload = messageWithString.getPayload();
        Class<?> aClass = payload.getClass();
        log.info("Received message of type {}", aClass.getName());

    }
}
