package io.github.fishthefirst.jmscontextprovider.exceptions;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

public class ExceptionPointer {
    private Exception lastException;
    private Instant lastExceptionLogTime;
    private final int exceptionLogDelay;

    public ExceptionPointer(int exceptionLogDelay) {
        if(exceptionLogDelay < 0) {
            exceptionLogDelay = 0;
        }
        this.exceptionLogDelay = exceptionLogDelay;
    }

    public boolean shouldLog(Exception e) {
        boolean b = Objects.isNull(lastExceptionLogTime)
                        || Objects.isNull(lastException)
                        || lastException.getClass() != e.getClass()
                        || Instant.now().isAfter(lastExceptionLogTime.plus(exceptionLogDelay, ChronoUnit.MILLIS));
        if(b) lastExceptionLogTime = Instant.now();
        lastException = e;
        return b;
    }

    public void clear() {
        lastExceptionLogTime = null;
        lastException = null;
    }
}
