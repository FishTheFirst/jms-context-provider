package io.github.fishthefirst.jmscontextprovider.utils;

import io.github.fishthefirst.jmscontextprovider.exceptions.ExceptionPointer;
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

    public static void tryAndLogError(Runnable r, ExceptionPointer exceptionPointer) {
        tryAndLogError(r, "", exceptionPointer);
    }

    public static void tryAndLogError(Runnable r, String exceptionMessage) {
        tryAndLogError(r, exceptionMessage, noop);
    }
    public static void tryAndLogError(Runnable r, String exceptionMessage, ExceptionPointer exceptionPointer) {
        tryAndLogError(r, exceptionMessage, noop, exceptionPointer);
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
            tryAndLogError(onException);
        }
    }

    public static void tryAndLogError(Runnable r, String exceptionMessage, Runnable onException, ExceptionPointer exceptionPointer) {
        try {
            r.run();
            exceptionPointer.clear();
        } catch (Exception e) {
            if(exceptionPointer.shouldLog(e)) {
                if (e instanceof JMSRuntimeException jmsRuntimeException) {
                    log.error(exceptionMessage, jmsRuntimeException.getCause());
                } else {
                    log.error(exceptionMessage, e);
                }
            }
            tryAndLogError(onException);
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

    public static <T> void tryAndLogError(T arg, Consumer<T> consumer, String exceptionMessage, Runnable onException, ExceptionPointer exceptionPointer) {
        try {
            consumer.accept(arg);
            exceptionPointer.clear();
        } catch (Exception e) {
            if(exceptionPointer.shouldLog(e)) {
                if (e instanceof JMSRuntimeException jmsRuntimeException) {
                    log.error(exceptionMessage, jmsRuntimeException.getCause());
                } else {
                    log.error(exceptionMessage, e);
                }
            }
            onException.run();
        }
    }
}
