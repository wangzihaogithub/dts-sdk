package com.github.dts.sdk.conf;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Properties;

@ConfigurationProperties(prefix = "server.dts.sdk")
public class DtsSdkConfig {
    private final ClusterConfig cluster = new ClusterConfig();
    private long requestRetrySleep = 6000L;
    private int requestMaxRetry = 10;

    public long getRequestRetrySleep() {
        return requestRetrySleep;
    }

    public void setRequestRetrySleep(long requestRetrySleep) {
        this.requestRetrySleep = requestRetrySleep;
    }

    public int getRequestMaxRetry() {
        return requestMaxRetry;
    }

    public void setRequestMaxRetry(int requestMaxRetry) {
        this.requestMaxRetry = requestMaxRetry;
    }

    public ClusterConfig getCluster() {
        return cluster;
    }

    public enum DiscoveryEnum {
        AUTO,
        REDIS,
        NACOS,
        YAML
    }

    public static class ClusterConfig {
        private final Redis redis = new Redis();
        private final Nacos nacos = new Nacos();
        private final Yaml yaml = new Yaml();
        private int testSocketTimeoutMs = 500;
        private DiscoveryEnum discovery = DiscoveryEnum.REDIS;

        public int getTestSocketTimeoutMs() {
            return testSocketTimeoutMs;
        }

        public void setTestSocketTimeoutMs(int testSocketTimeoutMs) {
            this.testSocketTimeoutMs = testSocketTimeoutMs;
        }

        public Yaml getYaml() {
            return yaml;
        }

        public Nacos getNacos() {
            return nacos;
        }

        public DiscoveryEnum getDiscovery() {
            return discovery;
        }

        public void setDiscovery(DiscoveryEnum discovery) {
            this.discovery = discovery;
        }

        public Redis getRedis() {
            return redis;
        }

        public static class Redis {
            private String redisConnectionFactoryBeanName = "redisConnectionFactory";
            private String redisKeyRootPrefix = "dts:${spring.profiles.active:def}";
            private int redisInstanceExpireSec = 10;
            private int messageIdIncrementDelta = 50;
            // 防止sub，pub命令有延迟，增加定时轮训
            private int updateInstanceTimerMs = 5000;

            public int getUpdateInstanceTimerMs() {
                return updateInstanceTimerMs;
            }

            public void setUpdateInstanceTimerMs(int updateInstanceTimerMs) {
                this.updateInstanceTimerMs = updateInstanceTimerMs;
            }

            public int getMessageIdIncrementDelta() {
                return messageIdIncrementDelta;
            }

            public void setMessageIdIncrementDelta(int messageIdIncrementDelta) {
                this.messageIdIncrementDelta = messageIdIncrementDelta;
            }

            public String getRedisConnectionFactoryBeanName() {
                return redisConnectionFactoryBeanName;
            }

            public void setRedisConnectionFactoryBeanName(String redisConnectionFactoryBeanName) {
                this.redisConnectionFactoryBeanName = redisConnectionFactoryBeanName;
            }

            public String getRedisKeyRootPrefix() {
                return redisKeyRootPrefix;
            }

            public void setRedisKeyRootPrefix(String redisKeyRootPrefix) {
                this.redisKeyRootPrefix = redisKeyRootPrefix;
            }

            public int getRedisInstanceExpireSec() {
                return redisInstanceExpireSec;
            }

            public void setRedisInstanceExpireSec(int redisInstanceExpireSec) {
                this.redisInstanceExpireSec = redisInstanceExpireSec;
            }

        }

        public static class Yaml {
            private final SdkAccount sdkAccount = new SdkAccount();
            private List<DtsServer> dtsServer;

            public List<DtsServer> getDtsServer() {
                return dtsServer;
            }

            public void setDtsServer(List<DtsServer> dtsServer) {
                this.dtsServer = dtsServer;
            }

            public SdkAccount getSdkAccount() {
                return sdkAccount;
            }
        }

        public static class Nacos {
            private String serverAddr = "${nacos.discovery.server-addr:${nacos.config.server-addr:${spring.cloud.nacos.server-addr:${spring.cloud.nacos.discovery.server-addr:${spring.cloud.nacos.config.server-addr:}}}}}";
            private String namespace = "${nacos.discovery.namespace:${nacos.config.namespace:${spring.cloud.nacos.namespace:${spring.cloud.nacos.discovery.namespace:${spring.cloud.nacos.config.namespace:}}}}}";
            private String serviceName = "${spring.application.name:sse-server}";
            private String clusterName = "${nacos.discovery.clusterName:${nacos.config.clusterName:${spring.cloud.nacos.clusterName:${spring.cloud.nacos.discovery.clusterName:${spring.cloud.nacos.config.clusterName:DEFAULT}}}}}";
            private Properties properties = new Properties();

            public Properties buildProperties() {
                Properties properties = new Properties();
                properties.putAll(this.properties);
                if (serverAddr != null && !serverAddr.isEmpty()) {
                    properties.put("serverAddr", serverAddr);
                }
                if (namespace != null && !namespace.isEmpty()) {
                    properties.put("namespace", namespace);
                }
                return properties;
            }

            public String getServerAddr() {
                return serverAddr;
            }

            public void setServerAddr(String serverAddr) {
                this.serverAddr = serverAddr;
            }

            public String getNamespace() {
                return namespace;
            }

            public void setNamespace(String namespace) {
                this.namespace = namespace;
            }

            public String getServiceName() {
                return serviceName;
            }

            public void setServiceName(String serviceName) {
                this.serviceName = serviceName;
            }

            public String getClusterName() {
                return clusterName;
            }

            public void setClusterName(String clusterName) {
                this.clusterName = clusterName;
            }

            public Properties getProperties() {
                return properties;
            }

            public void setProperties(Properties properties) {
                this.properties = properties;
            }
        }

    }

    public static class DtsServer {
        private String ip;
        private Integer port;

        public void validate() {
            if (ip == null || ip.isEmpty()) {
                throw new IllegalArgumentException("ip is empty");
            }
            if (port == null || port < 0) {
                throw new IllegalArgumentException("port is empty");
            }
        }

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }
    }

    public static class SdkAccount {
        private String account;
        private String password;

        public void validate() {
            if (account == null || account.isEmpty()) {
                throw new IllegalArgumentException("account is empty");
            }
            if (password == null || password.isEmpty()) {
                throw new IllegalArgumentException("password is empty");
            }
        }

        public String getAccount() {
            return account;
        }

        public void setAccount(String account) {
            this.account = account;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}
