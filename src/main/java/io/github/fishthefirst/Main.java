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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.activemq.ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
@Slf4j
public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory("admin", "admin", "tcp://localhost:62616");
        JMSMainContextHolder connectionFactory = JMSFactory.createContextHolder(activeMQConnectionFactory, INDIVIDUAL_ACKNOWLEDGE);
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        ObjectMapper objectMapper = new ObjectMapper();
        MessageWithMetadataJacksonMixInConfigurator.configureMixIn(objectMapper);
        JMSConsumerHolder consumer = JMSFactory.createConsumer(connectionFactory, Main::acceptMessage, (s) -> {
            try {
                return objectMapper.readValue(s, MessageWithMetadata.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }, "queue", false, "Consumer1");
        consumer.registerOnReadFailEventHandler(() -> {log.error("Read fail");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        consumer.registerOnUnmarshallFailEventHandler((s) -> log.error("Failed to unmarshall {}", s));
        consumer.registerOnReadTimeoutEventHandler(() -> log.info("Read timeout"));

        Future<?> daemon = executorService.submit(() -> {while(!Thread.currentThread().isInterrupted()) consumer.run();});

        Thread.sleep(3000);
        consumer.close();

        daemon.get();
    }

    public static void acceptMessage(MessageWithMetadata messageWithString) {
        Object payload = messageWithString.getPayload();
        Class<?> aClass = payload.getClass();
        log.info("Received message of type {}", aClass.getName());
    }
}
