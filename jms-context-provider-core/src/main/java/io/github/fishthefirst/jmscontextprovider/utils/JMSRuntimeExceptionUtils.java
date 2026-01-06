package io.github.fishthefirst.jmscontextprovider.utils;

import jakarta.jms.JMSRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public final class JMSRuntimeExceptionUtils {
    private static final Logger log = LoggerFactory.getLogger(JMSRuntimeExceptionUtils.class);
    private static final Runnable noop = () -> {};

    private JMSRuntimeExceptionUtils() {
    }

    public static void tryAndLogError(Runnable r) {
        tryAndLogError(r, "");
    }

    public static void tryAndLogError(Runnable r, String exceptionMessage) {
        tryAndLogError(r, exceptionMessage, noop);
    }

    public static void tryAndLogError(Runnable r, String exceptionMessage, Runnable onException) {
        try {
            r.run();
        } catch (Exception e) {
            if (e instanceof JMSRuntimeException jmsRuntimeException) {
                log.error(exceptionMessage, jmsRuntimeException.getCause());
            }
            else {
                log.error(exceptionMessage, e);
            }
            onException.run();
        }
    }

    public static <T> void tryAndLogError(T arg, Consumer<T> consumer) {
        tryAndLogError(arg, consumer, "", noop);
    }

    public static <T> void tryAndLogError(T arg, Consumer<T> consumer,  String exceptionMessage) {
        tryAndLogError(arg, consumer, exceptionMessage, noop);
    }

    public static <T> void tryAndLogError(T arg, Consumer<T> consumer, String exceptionMessage, Runnable onException) {
        try {
            consumer.accept(arg);
        } catch (Exception e) {
            if (e instanceof JMSRuntimeException jmsRuntimeException) {
                log.error(exceptionMessage, jmsRuntimeException.getCause());
            }
            else {
                log.error(exceptionMessage, e);
            }
            onException.run();
        }
    }
}
