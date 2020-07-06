package com.envisioniot.kafka.monitor.util;

import java.util.concurrent.*;

/**
 * @author qiang.bi
 * @date 2020/7/3 14:49
 **/
public class ThreadPoolUtils {

    private static final long THREADPOOL_KEEP_ALIVE_TIME = 10000L;

    public static ExecutorService getThreadPool(final String namePrefix,
                                                int coreSize,
                                                int maxPoolSize,
                                                int queueCapacity) {
        ThreadFactory namedThreadFactory = new ThreadFactory() {
            public Thread newThread(Runnable r) {
                //设置线程名称
                return new Thread(r,namePrefix);
            }
        };
        return new ThreadPoolExecutor(
                coreSize,
                maxPoolSize,
                THREADPOOL_KEEP_ALIVE_TIME,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(queueCapacity),
                namedThreadFactory,
                new ThreadPoolExecutor.AbortPolicy());
    }

    public static ExecutorService getSingleThreadPool(String namePrefix) {
        return getThreadPool(namePrefix, 1, 1, 16);
    }
}
