package io.github.fishthefirst;

import io.github.fishthefirst.jms.JMSProducerTransactionManager;
import io.github.fishthefirst.transactional.JMSTransactionContextAspect;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurationPackage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

@ConditionalOnClass(JMSTransactionContextAspect.class)
@AutoConfigurationPackage
@AutoConfiguration
public final class JMSTransactionalAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(JMSTransactionContextAspect.class)
    public JMSTransactionContextAspect jmsTransactionContextAspect(@Lazy JMSProducerTransactionManager transactionManager) {
        return new JMSTransactionContextAspect(transactionManager);
    }
}


