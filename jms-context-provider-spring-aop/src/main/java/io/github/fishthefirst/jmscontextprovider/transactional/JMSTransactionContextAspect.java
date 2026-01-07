package io.github.fishthefirst.jmscontextprovider.transactional;

import io.github.fishthefirst.jmscontextprovider.jms.JMSProducerTransactionManager;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

@Aspect
@Component
public final class JMSTransactionContextAspect {
    private final JMSProducerTransactionManager transactionManager;

    public JMSTransactionContextAspect(@Lazy JMSProducerTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    @Around("@annotation(JMSTransactional)")
    public Object wrapWithTransaction(ProceedingJoinPoint joinPoint, JMSTransactional jmsTransactional) throws Throwable {
        transactionManager.startTransaction();
        try {
            Object proceed = joinPoint.proceed();
            if(jmsTransactional.async()) {
                transactionManager.commitAsync();
            }
            else {
                transactionManager.commit();
            }
            return proceed;
        } catch (Throwable e) {
            transactionManager.rollback();
            throw e;
        }
    }
}
