package io.github.fishthefirst;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.fishthefirst.data.MessageWithMetadata;
import io.github.fishthefirst.jackson.MessageWithMetadataJacksonMixInConfigurator;
import io.github.fishthefirst.jms.JMSConsumerHolder;
import io.github.fishthefirst.jms.JMSFactory;
import io.github.fishthefirst.jms.JMSMainContextHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.activemq.ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
@Slf4j
public class Main {
    static JMSConsumerHolder consumer;
    public static void main(String[] args) throws InterruptedException {
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("admin", "admin", "tcp://localhost:62616");
        JMSMainContextHolder contextHolder = JMSFactory.createContextHolder(activeMQConnectionFactory, INDIVIDUAL_ACKNOWLEDGE);

        contextHolder.setClientId("clientId");
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        ObjectMapper objectMapper = new ObjectMapper();
        MessageWithMetadataJacksonMixInConfigurator.configureMixIn(objectMapper);
        consumer = JMSFactory.createConsumer(contextHolder, Main::acceptMessage, (s) -> {
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

        Future<?> daemon = executorService.submit(() -> {while(true) {
            try{consumer.run();} catch (RuntimeException ie) {if(ie.getCause() instanceof InterruptedException) return;}}
        });

        JMSConsumerHolder consumer2 = JMSFactory.createConsumer(contextHolder, Main::acceptMessage, (s) -> {
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

        Future<?> d2 = executorService.submit(() -> {while(true) {
            try{consumer2.run();} catch (RuntimeException ie) {if(ie.getCause() instanceof InterruptedException) return;}}
        });

        Thread.sleep(5000);
        executorService.shutdownNow();
        //daemon.cancel(true);
        executorService.awaitTermination(30, TimeUnit.SECONDS);

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
