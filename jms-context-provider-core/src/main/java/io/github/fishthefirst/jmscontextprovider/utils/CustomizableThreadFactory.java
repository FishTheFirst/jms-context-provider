package io.github.fishthefirst.jmscontextprovider.utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class CustomizableThreadFactory implements ThreadFactory {
    private static final ConcurrentHashMap<String, CustomizableThreadFactory> staticMap = new ConcurrentHashMap<>();
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String threadNamePrefix;

    public static CustomizableThreadFactory getInstance(Object object) {
        return getInstance(object.getClass().getSimpleName());
    }

    public static CustomizableThreadFactory getInstance(String threadNamePrefix) {
        return staticMap.computeIfAbsent(threadNamePrefix, CustomizableThreadFactory::new);
    }

    private CustomizableThreadFactory(String threadNamePrefix) {
        this.threadNamePrefix = threadNamePrefix;
    }

    public Thread newThread(Runnable runnable) {
        return new Thread(runnable, threadNamePrefix + "-" + threadNumber.getAndIncrement());
    }
}