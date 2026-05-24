package io.github.fishthefirst.jmscontextprovider.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.fishthefirst.jmscontextprovider.data.MessageWithMetadata;
import io.github.fishthefirst.jmscontextprovider.handlers.SendMessageAbortedHandler;
import io.github.fishthefirst.jmscontextprovider.handlers.SendMessageExceptionHandler;
import io.github.fishthefirst.jmscontextprovider.jms.JMSConnectionContextHolder;
import io.github.fishthefirst.jmscontextprovider.jms.JMSContextAwareComponentFactory;
import io.github.fishthefirst.jmscontextprovider.jms.JMSProducerTransactionManager;
import io.github.fishthefirst.jmscontextprovider.serde.MessageProcessor;
import io.github.fishthefirst.jmscontextprovider.serde.MessageWithMetadataMarshaller;
import io.github.fishthefirst.jmscontextprovider.serde.ObjectToStringMarshaller;
import io.github.fishthefirst.jmscontextprovider.serde.StringToObjectUnmarshaller;
import io.github.fishthefirst.jmscontextprovider.transactional.JMSTransactionContextAspect;
import jakarta.jms.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.lang.Nullable;

import java.time.Instant;

@ConditionalOnClass({JMSTransactionContextAspect.class, JMSConnectionContextHolder.class, JMSProducerTransactionManager.class, ConnectionFactory.class})
@AutoConfigurationPackage
@AutoConfiguration
public final class JMSTransactionalAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(JMSConnectionContextHolder.class)
    @ConditionalOnBean({ConnectionFactory.class, SessionModeSupplier.class})
    public JMSConnectionContextHolder jmsConnectionContextHolder(@Lazy ConnectionFactory connectionFactory,
                                                                 @Value("application.jms-context-provider.client-id:jms-provider") String clientId) {
        JMSConnectionContextHolder contextHolder = JMSContextAwareComponentFactory.createContextHolder(connectionFactory);
        contextHolder.setAllowContextWithoutClientId(false);
        contextHolder.setClientId(clientId);
        return contextHolder;
    }

    @Bean
    @ConditionalOnMissingBean(JMSProducerTransactionManager.class)
    @ConditionalOnBean({ObjectToStringMarshaller.class, JMSConnectionContextHolder.class})
    public <T> JMSProducerTransactionManager<T> jmsProducerTransactionManager(@Lazy JMSConnectionContextHolder connectionContextHolder,
                                                                       @Lazy ObjectToStringMarshaller<T> messageToStringMarshaller,
                                                                       @Nullable SendMessageExceptionHandler<T> sendMessageExceptionHandler,
                                                                       @Nullable SendMessageAbortedHandler<T> sendMessageAbortedHandler,
                                                                       @Nullable MessageProcessor<T> messagePreProcessor,
                                                                       @Nullable MessageProcessor<T> messagePostProcessor,
                                                                       @Value("application.jms-context-provider.destination-name") String destinationName,
                                                                       @Value("application.jms-context-provider.topic:false") boolean topic) {
        return new JMSProducerTransactionManager<>(connectionContextHolder,
                messageToStringMarshaller,
                sendMessageExceptionHandler,
                sendMessageAbortedHandler,
                messagePreProcessor,
                messagePostProcessor,
                destinationName,
                topic);
    }

    @Bean
    @ConditionalOnMissingBean(JMSTransactionContextAspect.class)
    public JMSTransactionContextAspect jmsTransactionContextAspect(@Lazy JMSProducerTransactionManager<?> transactionManager) {
        return new JMSTransactionContextAspect(transactionManager);
    }

    @Bean
    @ConditionalOnMissingBean(ObjectToStringMarshaller.class)
    public ObjectToStringMarshaller<?> objectToStringMarshaller(@Lazy ObjectMapper objectMapper) {
        return new MessageWithMetadataMarshaller<>((o) -> {
            try {
                return new MessageWithMetadata(Instant.now(), objectMapper.writeValueAsString(o), o.getClass().getName()).toStringPayload();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, false);
    }

    @Bean
    @ConditionalOnMissingBean(StringToObjectUnmarshaller.class)
    public StringToObjectUnmarshaller<?> stringToObjectUnmarshaller(@Lazy ObjectMapper objectMapper) {
        return (s) -> {
            MessageWithMetadata message = new MessageWithMetadata(s);
            try {
                return objectMapper.readValue(message.getPayload(), message.getPayloadClass());
            } catch (JsonProcessingException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        };
    }
}