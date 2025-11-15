package io.github.fishthefirst.utils;

import jakarta.jms.JMSRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JMSRuntimeExceptionUtils {
    private static final Logger log = LoggerFactory.getLogger(JMSRuntimeExceptionUtils.class);

    private JMSRuntimeExceptionUtils() {
    }

    public static void tryAndLogError(Runnable r) {
        tryAndLogError(r, "");
    }

    public static void tryAndLogError(Runnable r, String msg) {
        tryAndLogError(r, msg, () -> {});
    }

    public static void tryAndLogError(Runnable r, String msg, Runnable onException) {
        try {
            r.run();
        } catch (Exception e) {
            if (e instanceof JMSRuntimeException jmsRuntimeException)
                log.error(msg, jmsRuntimeException.getCause());
            else {
                log.error(msg, e);
            }
            onException.run();
        }
    }
}
