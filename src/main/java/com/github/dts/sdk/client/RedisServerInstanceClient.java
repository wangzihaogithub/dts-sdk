package com.github.dts.sdk.client;

import com.github.dts.sdk.conf.DtsSdkConfig;

public class RedisServerInstanceClient extends ServerInstanceClient {

    public RedisServerInstanceClient(boolean socketConnected, SdkInstance sdkInstance, ServerInstance serverInstance, DtsSdkConfig.ClusterConfig clusterConfig) {
        super(socketConnected, sdkInstance, serverInstance, clusterConfig);
    }
}
