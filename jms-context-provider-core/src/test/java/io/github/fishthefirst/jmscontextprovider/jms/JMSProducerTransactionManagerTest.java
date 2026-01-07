package io.github.fishthefirst.jmscontextprovider.jms;

import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.TextMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JMSProducerTransactionManagerTest {
    private JMSProducerTransactionManager jmsProducerTransactionManager;
    private JMSConnectionContextHolder connectionContextHolder;

    @BeforeEach
    public void setup() {
        connectionContextHolder = Mockito.mock(JMSConnectionContextHolder.class);
        jmsProducerTransactionManager = new JMSProducerTransactionManager(
                connectionContextHolder,
                Object::toString,
                null,
                null,
                "destination",
                true);
    }

    @Test
    public void sendObjectFailDuringTransaction_Abort() {
        JMSContextWrapper contextWrapperMock = Mockito.mock(JMSContextWrapper.class);
        JMSContext jmsContextMock = Mockito.mock(JMSContext.class);
        JMSProducer jmsProducer = Mockito.mock(JMSProducer.class);

        when(connectionContextHolder.createContext(anyInt(), any())).thenReturn(contextWrapperMock);
        when(contextWrapperMock.getContext()).thenReturn(jmsContextMock);
        when(jmsContextMock.createProducer()).thenReturn(jmsProducer);

        jmsProducerTransactionManager.startTransaction();
        jmsProducerTransactionManager.sendObject("Object 1");

        when(jmsProducer.send(nullable(Destination.class), nullable(TextMessage.class))).thenThrow(new JMSRuntimeException(""));

        jmsProducerTransactionManager.sendObject("Object 2");
        jmsProducerTransactionManager.sendObject("Object 3");
        jmsProducerTransactionManager.commit();
        verify(jmsContextMock, times(1)).createProducer();

        jmsProducerTransactionManager.startTransaction();
        jmsProducerTransactionManager.sendObject("Object 4");
        verify(jmsContextMock, times(2)).createProducer();
    }

    @Test
    public void sendObjectFailDuringNonTransaction_NoAbort() {
        JMSContextWrapper contextWrapperMock = Mockito.mock(JMSContextWrapper.class);
        JMSContext jmsContextMock = Mockito.mock(JMSContext.class);
        JMSProducer jmsProducer = Mockito.mock(JMSProducer.class);

        when(connectionContextHolder.createContext(anyInt(), any())).thenReturn(contextWrapperMock);
        when(contextWrapperMock.getContext()).thenReturn(jmsContextMock);
        when(jmsContextMock.createProducer()).thenReturn(jmsProducer);

        jmsProducerTransactionManager.sendObject("Object 1");

        when(jmsProducer.send(nullable(Destination.class), nullable(TextMessage.class))).thenThrow(new JMSRuntimeException(""));

        jmsProducerTransactionManager.sendObject("Object 2");
        jmsProducerTransactionManager.sendObject("Object 3");
        verify(jmsContextMock, times(3)).createProducer();

        jmsProducerTransactionManager.sendObject("Object 4");
        verify(jmsContextMock, times(4)).createProducer();
    }
}