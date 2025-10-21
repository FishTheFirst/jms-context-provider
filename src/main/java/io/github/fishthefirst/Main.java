package io.github.fishthefirst;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.jms.JMSContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.ActiveMQConnectionFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
@Slf4j
public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        JMSContextProvider connectionFactory = JMSFactory.createProvider(new ActiveMQConnectionFactory("admin", "admin", "tcp://localhost:61616"), JMSContext.SESSION_TRANSACTED);
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        ObjectMapper objectMapper = new ObjectMapper();

        EventDrivenJMSConsumer consumer = JMSFactory.createConsumer(connectionFactory, (s) -> log.info("Received message {}", s), (s) -> {
            try {
                return objectMapper.readValue(s, MessageWithMetadata.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }, "queue", false, "Consumer1");
        consumer.registerOnReadFailEventHandler(() -> log.info("Read fail/timeout"));
        consumer.registerOnUnmarshallFailEventHandler((s) -> log.error("Failed to unmarshall {}", s));

//        LMSConsumer consumer2 = LMSMQFactory.createConsumer(connectionFactory, (s) -> log.info("Received message {}", s), (s) -> {
//            try {
//                return objectMapper.readValue(s, MessageWithMetadata.class);
//            } catch (JsonProcessingException e) {
//                throw new RuntimeException(e);
//            }
//        }, "queue", false, "Consumer2");
//        consumer2.registerOnReadFailEventHandler(() -> log.info("Read fail/timeout"));
//        consumer2.registerOnUnmarshallFailEventHandler((s) -> log.error("Failed to unmarshall {}", s));

        Future<?> daemon = executorService.submit(() -> {while(true) consumer.run();});
        //Future<?> slave = executorService.submit(() -> {while(true) consumer2.run();});
        //CompletableFuture<Void> daemon = CompletableFuture.runAsync(consumer, executorService);
        //CompletableFuture<Void> slave = CompletableFuture.runAsync(consumer2, executorService);
        //consumer2.close();
        //consumer.close();
        //slave.get();
        daemon.get();
    }

}
