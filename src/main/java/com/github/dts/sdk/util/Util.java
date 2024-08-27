package com.github.dts.sdk.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.stream.Stream;

public class Util {

    private static final Logger log = LoggerFactory.getLogger(Util.class);
    private static String ipAddress;

    public static <E extends Throwable> void sneakyThrows(Throwable t) throws E {
        throw (E) t;
    }

    public static String encodeBasicAuth(String username, String password, Charset charset) {
        String credentialsString = username + ":" + password;
        byte[] encodedBytes = Base64.getEncoder().encode(credentialsString.getBytes(charset));
        return new String(encodedBytes, charset);
    }

    public static boolean isNotEmpty(Collection collection) {
        return collection != null && !collection.isEmpty();
    }

    public static boolean isBlank(String str) {
        int strLen;
        if (str != null && (strLen = str.length()) != 0) {
            for (int i = 0; i < strLen; ++i) {
                if (!Character.isWhitespace(str.charAt(i))) {
                    return false;
                }
            }

            return true;
        } else {
            return true;
        }
    }

    public static boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static String getIPAddress() {
        if (ipAddress != null) {
            return ipAddress;
        } else {
            try {
                Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
                String[] skipNames = {"TAP", "VPN", "UTUN", "VIRBR"};
                while (networkInterfaces.hasMoreElements()) {
                    NetworkInterface networkInterface = networkInterfaces.nextElement();
                    if (networkInterface.isVirtual() && networkInterface.isLoopback()) {
                        continue;
                    }

                    String name = Objects.toString(networkInterface.getName(), "").trim().toUpperCase();
                    String displayName = Objects.toString(networkInterface.getDisplayName(), "").trim().toUpperCase();
                    String netName = name.length() > 0 ? name : displayName;
                    boolean skip = Stream.of(skipNames).anyMatch(netName::contains);
                    if (skip) {
                        continue;
                    }

                    Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                    while (inetAddresses.hasMoreElements()) {
                        InetAddress inetAddress = inetAddresses.nextElement();
                        if (inetAddress.isLoopbackAddress() || !inetAddress.isSiteLocalAddress()) {
                            continue;
                        }
                        String hostAddress = inetAddress.getHostAddress();
                        return ipAddress = hostAddress;
                    }
                }
                // 如果没有发现 non-loopback地址.只能用最次选的方案
                return ipAddress = InetAddress.getLocalHost().getHostAddress();
            } catch (Exception var6) {
                return null;
            }
        }
    }

    public static Object getBean(BeanFactory beanFactory, String beanName, Class type) {
        Object redisConnectionFactory;
        try {
            redisConnectionFactory = beanFactory.getBean(beanName);
        } catch (BeansException e) {
            redisConnectionFactory = beanFactory.getBean(type);
        }
        return redisConnectionFactory;
    }

    public static ScheduledExecutorService newScheduled(int nThreads, String name, boolean wrapper) {
        return new ScheduledThreadPoolExecutor(
                nThreads,
                new CustomizableThreadFactory(name) {
                    @Override
                    public Thread newThread(Runnable runnable) {
                        Thread thread = super.newThread(wrapper ? () -> {
                            try {
                                runnable.run();
                            } catch (Exception e) {
                                log.warn("Scheduled error {}", e, e);
                                throw e;
                            }
                        } : runnable);
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                (r, exe) -> {
                    if (!exe.isShutdown()) {
                        try {
                            exe.getQueue().put(r);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                });
    }

    public static ThreadPoolExecutor newFixedThreadPool(int nThreads, long keepAliveTime, String name, boolean wrapper) {
        return newFixedThreadPool(nThreads, nThreads, keepAliveTime, name, wrapper, true);
    }

    public static ThreadPoolExecutor newFixedThreadPool(int core, int nThreads, long keepAliveTime, String name, boolean wrapper, boolean allowCoreThreadTimeOut) {
        return newFixedThreadPool(core, nThreads, keepAliveTime, name, wrapper, allowCoreThreadTimeOut, 0);
    }

    public static ThreadPoolExecutor newFixedThreadPool(int core, int nThreads, long keepAliveTime, String name, boolean wrapper, boolean allowCoreThreadTimeOut, int queues) {
        return newFixedThreadPool(core, nThreads, keepAliveTime, name, wrapper, allowCoreThreadTimeOut, queues, null);
    }

    public static <E extends Runnable> ThreadPoolExecutor newFixedThreadPool(int core, int nThreads, long keepAliveTime, String name, boolean wrapper, boolean allowCoreThreadTimeOut, int queues, BiFunction<E, E, E> mergeFunction) {
        BlockingQueue<Runnable> workQueue = queues == 0 ?
                new SynchronousQueue<>() :
                (queues < 0 ? new LinkedBlockingQueue<>(Integer.MAX_VALUE)
                        : new LinkedBlockingQueue<>(queues));
        ThreadPoolExecutor executor = new ThreadPoolExecutor(core,
                nThreads,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                workQueue,
                new CustomizableThreadFactory(name) {
                    @Override
                    public Thread newThread(Runnable runnable) {
                        return super.newThread(wrapper ? () -> {
                            try {
                                runnable.run();
                            } catch (Exception e) {
                                log.warn("error {}", e, e);
                                throw e;
                            }
                        } : runnable);
                    }
                }, new ThreadPoolExecutor.CallerRunsPolicy());
        executor.allowCoreThreadTimeOut(allowCoreThreadTimeOut);
        return executor;
    }

    public static ThreadPoolExecutor newSingleThreadExecutor(long keepAliveTime) {
        return new ThreadPoolExecutor(1,
                1,
                keepAliveTime,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                (r, exe) -> {
                    if (!exe.isShutdown()) {
                        try {
                            exe.getQueue().put(r);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                    }
                });
    }

    public static boolean isDefaultRedisProps(Environment env) {
        String[] props = new String[]{
                "spring.redis.url",
                "spring.redis.host",
                "spring.redis.port",
                "spring.redis.database",
                "spring.redis.username",
                "spring.redis.password",
                "spring.redis.cluster.nodes",
                "spring.redis.sentinel.nodes"
        };
        for (String prop : props) {
            if (env.containsProperty(prop)) {
                return false;
            }
        }
        return true;
    }
}
