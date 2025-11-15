package io.github.fishthefirst.utils;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class WatchdogTimer {
    private int lastValue;
    private final ScheduledExecutorService timeoutWatchdog = Executors.newSingleThreadScheduledExecutor(CustomizableThreadFactory.getInstance(this.getClass().getSimpleName()));
    private final Runnable callback;
    private ScheduledFuture<?> timeoutFuture;

    public WatchdogTimer(Runnable callback) {
        this.callback = callback;
    }

    public void close() {
        stop();
        timeoutWatchdog.shutdownNow();
    }

    public synchronized void start(int newValue) {
        lastValue = newValue;
        stop();
        timeoutFuture = timeoutWatchdog.schedule(callback, lastValue, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        if (Objects.nonNull(timeoutFuture)) {
            timeoutFuture.cancel(false);
        }
        timeoutFuture = null;
    }

    public synchronized void reset() {
        stop();
        start(lastValue);
    }
}
