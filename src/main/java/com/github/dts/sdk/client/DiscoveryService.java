package com.github.dts.sdk.client;

import com.github.dts.sdk.conf.DtsSdkConfig;
import com.github.dts.sdk.util.PlatformDependentUtil;
import com.github.dts.sdk.util.ReferenceCounted;
import com.github.dts.sdk.util.Util;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.env.Environment;

import java.util.List;

public interface DiscoveryService {

    static DiscoveryService newInstance(DtsSdkConfig.ClusterConfig config,
                                        ListableBeanFactory beanFactory) {
        DtsSdkConfig.DiscoveryEnum discoveryEnum = config.getDiscovery();
        if (discoveryEnum == DtsSdkConfig.DiscoveryEnum.AUTO) {
            if (PlatformDependentUtil.isSupportSpringframeworkRedis()
                    && beanFactory.getBeanNamesForType(PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS).length > 0
                    && !Util.isDefaultRedisProps(beanFactory.getBean(Environment.class))) {
                discoveryEnum = DtsSdkConfig.DiscoveryEnum.REDIS;
            } else if (Util.isNotBlank(config.getYaml().getSdkAccount().getAccount())
                    && Util.isNotBlank(config.getYaml().getSdkAccount().getPassword())
                    && Util.isNotEmpty(config.getYaml().getDtsServer())) {
                discoveryEnum = DtsSdkConfig.DiscoveryEnum.YAML;
            }
        }
        switch (discoveryEnum) {
            case REDIS: {
                Environment env = beanFactory.getBean(Environment.class);
                String ip = Util.getIPAddress();
                Integer port = env.getProperty("server.port", Integer.class, 8080);
                DtsSdkConfig.ClusterConfig.Redis redis = config.getRedis();
                String redisKeyRootPrefix = redis.getRedisKeyRootPrefix();
                if (redisKeyRootPrefix != null) {
                    redisKeyRootPrefix = env.resolvePlaceholders(redisKeyRootPrefix);
                }
                Object redisConnectionFactory = Util.getBean(beanFactory, redis.getRedisConnectionFactoryBeanName(), PlatformDependentUtil.REDIS_CONNECTION_FACTORY_CLASS);
                return new RedisDiscoveryService(
                        redisConnectionFactory,
                        redisKeyRootPrefix,
                        redis.getRedisInstanceExpireSec(),
                        config, ip, port);
            }
            case YAML: {
                return new YamlDiscoveryService(config);
            }
            case NACOS: {
                throw new IllegalArgumentException("current version is not support nacos!");
            }
            default: {
                throw new IllegalArgumentException("ServiceDiscoveryService newInstance fail! remote discovery config is empty!");
            }
        }
    }

    void registerSdkInstance();

    <E extends ServerInstanceClient> ReferenceCounted<List<E>> getServerListRef();

    void addServerListener(ServerListener serverListener);

    interface ServerListener {
        <E extends ServerInstanceClient> void onChange(ServerChangeEvent<E> event);
    }

    class ServerChangeEvent<E extends ServerInstanceClient> {
        public final List<E> insertList;
        public final List<E> deleteList;
        // 首次=0，从0开始计数
        public int updateInstanceCount;

        public ServerChangeEvent(int updateInstanceCount,
                                 List<E> insertList, List<E> deleteList) {
            this.updateInstanceCount = updateInstanceCount;
            this.insertList = insertList;
            this.deleteList = deleteList;
        }

    }

}
