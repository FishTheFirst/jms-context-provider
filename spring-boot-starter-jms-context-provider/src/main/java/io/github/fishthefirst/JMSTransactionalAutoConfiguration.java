package io.github.fishthefirst;

import io.github.fishthefirst.handlers.SendMessageExceptionHandler;
import io.github.fishthefirst.jms.JMSConnectionContextHolder;
import io.github.fishthefirst.jms.JMSContextAwareComponentFactory;
import io.github.fishthefirst.jms.JMSProducerTransactionManager;
import io.github.fishthefirst.serde.MessagePreprocessor;
import io.github.fishthefirst.serde.ObjectToStringMarshaller;
import io.github.fishthefirst.transactional.JMSTransactionContextAspect;
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

import java.util.Objects;

@ConditionalOnClass({JMSTransactionContextAspect.class, JMSConnectionContextHolder.class, JMSProducerTransactionManager.class, ConnectionFactory.class})
@AutoConfigurationPackage
@AutoConfiguration
public final class JMSTransactionalAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(JMSConnectionContextHolder.class)
    @ConditionalOnBean({ConnectionFactory.class, SessionModeSupplier.class})
    public JMSConnectionContextHolder jmsConnectionContextHolder(@Lazy ConnectionFactory connectionFactory,
                                                                 SessionModeSupplier sessionModeSupplier,
                                                                 @Value("application.jms-context-provider.client-id:jms-provider") String clientId) {
        JMSConnectionContextHolder contextHolder = JMSContextAwareComponentFactory.createContextHolder(connectionFactory, sessionModeSupplier.sessionMode());
        contextHolder.setClientId(clientId);
        return contextHolder;
    }

    @Bean
    @ConditionalOnMissingBean(JMSProducerTransactionManager.class)
    @ConditionalOnBean({ObjectToStringMarshaller.class, JMSConnectionContextHolder.class})
    public JMSProducerTransactionManager jmsProducerTransactionManager(@Lazy JMSConnectionContextHolder connectionContextHolder,
                                                                       @Lazy ObjectToStringMarshaller messageToStringMarshaller,
                                                                       @Nullable SendMessageExceptionHandler sendMessageExceptionHandler,
                                                                       @Nullable MessagePreprocessor messagePreprocessor,
                                                                       @Value("application.jms-context-provider.destination-name") String destinationName,
                                                                       @Value("application.jms-context-provider.topic:false") boolean topic) {
        return new JMSProducerTransactionManager(connectionContextHolder, messageToStringMarshaller, Objects.requireNonNullElse(sendMessageExceptionHandler, (message) -> {}), Objects.requireNonNullElse(messagePreprocessor, message -> {}),destinationName, topic, false);
    }

    @Bean
    @ConditionalOnMissingBean(JMSTransactionContextAspect.class)
    public JMSTransactionContextAspect jmsTransactionContextAspect(@Lazy JMSProducerTransactionManager transactionManager) {
        return new JMSTransactionContextAspect(transactionManager);
    }
}


