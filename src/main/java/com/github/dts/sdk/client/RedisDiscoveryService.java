package com.github.dts.sdk.client;

import com.github.dts.sdk.conf.DtsSdkConfig;
import com.github.dts.sdk.util.DifferentComparatorUtil;
import com.github.dts.sdk.util.ReferenceCounted;
import com.github.dts.sdk.util.SnowflakeIdWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class RedisDiscoveryService implements DiscoveryService, DisposableBean {
    public static final String DEVICE_ID = SnowflakeIdWorker.INSTANCE.nextId() + "";
    private static final Logger log = LoggerFactory.getLogger(RedisDiscoveryService.class);
    private static final int MIN_REDIS_INSTANCE_EXPIRE_SEC = 2;
    private static volatile ScheduledExecutorService scheduled;
    private final int redisInstanceExpireSec;
    private final byte[] keySdkPubSubBytes;
    private final byte[] keyServerPubSubBytes;
    private final byte[] keyServerPubUnsubBytes;
    private final byte[] keySdkSetBytes;
    private final ScanOptions keyServerSetScanOptions;
    private final MessageListener messageServerListener;
    private final Jackson2JsonRedisSerializer<ServerInstance> instanceServerSerializer = new Jackson2JsonRedisSerializer<>(ServerInstance.class);
    private final DtsSdkConfig.ClusterConfig clusterConfig;
    private final RedisTemplate<byte[], byte[]> redisTemplate = new RedisTemplate<>();
    private final SdkInstance sdkInstance;
    private final byte[] sdkInstanceBytes;
    private final ReferenceCounted<List<ServerInstanceClient>> serverInstanceClientListRef = new ReferenceCounted<>(new CopyOnWriteArrayList<>());
    private final Collection<ServerListener> serverListenerList = new CopyOnWriteArrayList<>();
    private final int updateInstanceTimerMs;
    private long serverHeartbeatCount;
    private ScheduledFuture<?> sdkHeartbeatScheduledFuture;
    private ScheduledFuture<?> updateServerInstanceScheduledFuture;
    private boolean destroy;
    private Map<String, ServerInstance> serverInstanceMap = Collections.emptyMap();
    private int serverUpdateInstanceCount;

    public RedisDiscoveryService(Object redisConnectionFactory,
                                 String redisKeyRootPrefix,
                                 int redisInstanceExpireSec,
                                 DtsSdkConfig.ClusterConfig clusterConfig,
                                 String ip,
                                 Integer port) {
        Jackson2JsonRedisSerializer<SdkInstance> instanceSdkSerializer = new Jackson2JsonRedisSerializer<>(SdkInstance.class);
        String account = "sdk" + DEVICE_ID;
        SdkInstance sdkInstance = new SdkInstance();
        sdkInstance.setDeviceId(DEVICE_ID);
        sdkInstance.setAccount(account);
        sdkInstance.setPassword(UUID.randomUUID().toString().replace("-", ""));
        sdkInstance.setIp(ip);
        sdkInstance.setPort(port);
        this.sdkInstance = sdkInstance;
        this.sdkInstanceBytes = instanceSdkSerializer.serialize(sdkInstance);

        this.updateInstanceTimerMs = clusterConfig.getRedis().getUpdateInstanceTimerMs();
        this.redisInstanceExpireSec = Math.max(redisInstanceExpireSec, MIN_REDIS_INSTANCE_EXPIRE_SEC);
        this.clusterConfig = clusterConfig;

        StringRedisSerializer keySerializer = StringRedisSerializer.UTF_8;
        this.keySdkSetBytes = keySerializer.serialize(redisKeyRootPrefix + "sdk:ls:" + DEVICE_ID);
        this.keySdkPubSubBytes = keySerializer.serialize(redisKeyRootPrefix + "sdk:mq:sub");

        this.keyServerPubSubBytes = keySerializer.serialize(redisKeyRootPrefix + "svr:mq:sub");
        this.keyServerPubUnsubBytes = keySerializer.serialize(redisKeyRootPrefix + "svr:mq:unsub");
        this.keyServerSetScanOptions = ScanOptions.scanOptions()
                .count(20)
                .match(redisKeyRootPrefix + "svr:ls:*")
                .build();

        this.messageServerListener = (message, pattern) -> {
            if (this.destroy) {
                return;
            }
            byte[] channel = message.getChannel();
            if (Arrays.equals(channel, keyServerPubSubBytes)) {
                updateServerInstance(getServerInstanceMap());
            } else if (Arrays.equals(channel, keyServerPubUnsubBytes)) {
                updateServerInstance(getServerInstanceMap());
            }
        };

        this.redisTemplate.setConnectionFactory((RedisConnectionFactory) redisConnectionFactory);
        this.redisTemplate.afterPropertiesSet();
    }

    private static ScheduledExecutorService getScheduled() {
        if (scheduled == null) {
            synchronized (RedisDiscoveryService.class) {
                if (scheduled == null) {
                    scheduled = new ScheduledThreadPoolExecutor(1, r -> {
                        Thread result = new Thread(r, "RedisDiscoveryServiceHeartbeat");
                        result.setDaemon(true);
                        return result;
                    });
                }
            }
        }
        return scheduled;
    }

    public List<ServerInstanceClient> newServerInstanceClient(Collection<ServerInstance> instanceList) {
        int size = instanceList.size();
        List<ServerInstanceClient> list = new ArrayList<>(size);
        for (ServerInstance instance : instanceList) {
            boolean socketConnected = ServerInstance.isSocketConnected(instance, clusterConfig.getTestSocketTimeoutMs());
            try {
                ServerInstanceClient service = new ServerInstanceClient(socketConnected, sdkInstance, instance, clusterConfig);
                list.add(service);
            } catch (Exception e) {
                throw new IllegalStateException(
                        String.format("newServerInstanceClient  fail!  account = '%s', IP = '%s', port = %d ",
                                instance.getAccount(), instance.getIp(), instance.getPort()), e);
            }
        }
        return list;
    }

    @Override
    public void registerSdkInstance() {
        // server : set, pub, sub, get
        Map<String, ServerInstance> serverInstanceMap = redisTemplate.execute(connection -> {
            redisSetSdkInstance(connection);
            connection.publish(keySdkPubSubBytes, sdkInstanceBytes);
            connection.subscribe(messageServerListener, keyServerPubSubBytes, keyServerPubUnsubBytes);
            return getServerInstanceMap(connection);
        }, true);

        updateServerInstance(serverInstanceMap);
        this.updateServerInstanceScheduledFuture = scheduledUpdateServerInstance();
        this.sdkHeartbeatScheduledFuture = scheduledSdkHeartbeat();
    }

    @Override
    public void addServerListener(ServerListener serverListener) {
        serverListenerList.add(serverListener);
    }

    @Override
    public ReferenceCounted<List<ServerInstanceClient>> getServerListRef() {
        while (true) {
            try {
                return serverInstanceClientListRef.open();
            } catch (IllegalStateException ignored) {

            }
        }
    }

    private ScheduledFuture<?> scheduledUpdateServerInstance() {
        if (updateInstanceTimerMs <= 0) {
            return null;
        }
        ScheduledFuture<?> scheduledFuture = this.updateServerInstanceScheduledFuture;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        return getScheduled().scheduleWithFixedDelay(() -> updateServerInstance(getServerInstanceMap()), updateInstanceTimerMs, updateInstanceTimerMs, TimeUnit.MILLISECONDS);
    }

    private ScheduledFuture<?> scheduledSdkHeartbeat() {
        ScheduledFuture<?> scheduledFuture = this.sdkHeartbeatScheduledFuture;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        int delay;
        if (redisInstanceExpireSec == MIN_REDIS_INSTANCE_EXPIRE_SEC) {
            delay = 500;
        } else {
            delay = (redisInstanceExpireSec * 1000) / 3;
        }
        return getScheduled().scheduleWithFixedDelay(() -> {
            redisTemplate.execute(connection -> {
                // 续期过期时间
                Boolean success = connection.expire(keySdkSetBytes, redisInstanceExpireSec);
                if (success == null || !success) {
                    redisSetSdkInstance(connection);
                }
                serverHeartbeatCount++;
                return null;
            }, true);
        }, delay, delay, TimeUnit.MILLISECONDS);
    }

    private Boolean redisSetSdkInstance(RedisConnection connection) {
        return connection.set(keySdkSetBytes, sdkInstanceBytes, Expiration.seconds(redisInstanceExpireSec), RedisStringCommands.SetOption.UPSERT);
    }

    public synchronized void updateServerInstance(Map<String, ServerInstance> serverInstanceMap) {
        if (serverInstanceMap == null) {
            // serverInstanceMap == null is Redis IOException
            return;
        }
        DifferentComparatorUtil.ListDiffResult<String> diff = DifferentComparatorUtil.listDiff(this.serverInstanceMap.keySet(), serverInstanceMap.keySet());
        if (diff.isEmpty()) {
            return;
        }
        List<ServerInstance> insertList = diff.getInsertList().stream().map(serverInstanceMap::get).collect(Collectors.toList());
        List<ServerInstance> deleteList = diff.getDeleteList().stream().map(this.serverInstanceMap::get).collect(Collectors.toList());
        log.info("updateServerInstance insert={}, delete={}", insertList, deleteList);
        List<ServerInstanceClient> deleteClientList;
        List<ServerInstanceClient> insertClientList;
        List<ServerInstanceClient> refs = serverInstanceClientListRef.get();
        if (deleteList.isEmpty()) {
            deleteClientList = Collections.emptyList();
        } else {
            Set<String> deleteAccountSet = deleteList.stream().map(ServerInstance::getAccount).collect(Collectors.toSet());
            deleteClientList = refs.stream().filter(e -> deleteAccountSet.contains(e.getAccount())).collect(Collectors.toList());
            for (ServerInstanceClient client : deleteClientList) {
                client.discoveryClose();
            }
            refs.removeIf(next -> {
                for (ServerInstanceClient client : deleteClientList) {
                    if (next == client) {
                        return true;
                    }
                }
                return false;
            });
        }
        if (insertList.isEmpty()) {
            insertClientList = Collections.emptyList();
        } else {
            insertClientList = newServerInstanceClient(insertList);
            refs.addAll(insertClientList);
        }
        try {
            notifyServerChangeEvent(insertClientList, deleteClientList);
        } catch (Exception e) {
            log.warn("updateServerInstance notifyChangeEvent error {}", e.toString(), e);
        }
        this.serverInstanceMap = serverInstanceMap;
        this.serverUpdateInstanceCount++;
    }

    private void notifyServerChangeEvent(List<ServerInstanceClient> insertList,
                                         List<ServerInstanceClient> deleteList) {
        if (serverListenerList.isEmpty()) {
            return;
        }
        ServerChangeEvent<ServerInstanceClient> event = new ServerChangeEvent<>(serverUpdateInstanceCount, insertList, deleteList);
        for (ServerListener listener : serverListenerList) {
            listener.onChange(event);
        }
    }

    public Map<String, ServerInstance> getServerInstanceMap() {
        RedisCallback<Map<String, ServerInstance>> redisCallback = this::getServerInstanceMap;
        return redisTemplate.execute(redisCallback);
    }

    public Map<String, ServerInstance> getServerInstanceMap(RedisConnection connection) {
        Map<String, ServerInstance> map = new LinkedHashMap<>();
        try (Cursor<byte[]> cursor = connection.scan(keyServerSetScanOptions)) {
            while (cursor.hasNext()) {
                byte[] key = cursor.next();
                if (key == null) {
                    continue;
                }
                byte[] body = connection.get(key);
                if (body == null) {
                    continue;
                }
                ServerInstance instance = instanceServerSerializer.deserialize(body);
                if (instance != null) {
                    map.put(instance.getAccount(), instance);
                }
            }
        } catch (Exception ignored) {
            return null;
        }
        return map;
    }

    @Override
    public void destroy() {
        this.destroy = true;
        redisTemplate.execute(connection -> {
            connection.expire(keySdkSetBytes, 0);
            connection.publish(keyServerPubUnsubBytes, sdkInstanceBytes);
            return null;
        }, true);
    }

}
