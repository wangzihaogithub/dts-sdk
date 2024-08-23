package com.github.dts.sdk.client;

import com.github.dts.sdk.conf.DtsSdkConfig;
import com.github.dts.sdk.util.ReferenceCounted;
import com.github.dts.sdk.util.SnowflakeIdWorker;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class YamlDiscoveryService implements DiscoveryService {
    public static final String DEVICE_ID = SnowflakeIdWorker.INSTANCE.nextId() + "";
    private final DtsSdkConfig.ClusterConfig config;
    private final ReferenceCounted<List<ServerInstanceClient>> serverListRef;

    public YamlDiscoveryService(DtsSdkConfig.ClusterConfig config) {
        this.config = config;
        DtsSdkConfig.ClusterConfig.Yaml yaml = config.getYaml();

        List<DtsSdkConfig.DtsServer> dtsServerList = yaml.getDtsServer();
        for (DtsSdkConfig.DtsServer dtsServer : dtsServerList) {
            dtsServer.validate();
        }
        DtsSdkConfig.SdkAccount sdkAccount = yaml.getSdkAccount();

        yaml.getSdkAccount().validate();
        List<ServerInstance> instanceList = serverInstanceList(dtsServerList);
        List<ServerInstanceClient> clientList = serverInstanceClientList(sdkAccount, instanceList);
        serverListRef = new ReferenceCounted<>(new CopyOnWriteArrayList<>(clientList));
    }

    private List<ServerInstance> serverInstanceList(List<DtsSdkConfig.DtsServer> sdkAccount) {
        return sdkAccount.stream().map(e -> {
            ServerInstance instance = new ServerInstance();
            instance.setIp(e.getIp());
            instance.setPort(e.getPort());
            return instance;
        }).collect(Collectors.toList());
    }

    private List<ServerInstanceClient> serverInstanceClientList(DtsSdkConfig.SdkAccount sdkAccount, List<ServerInstance> serverInstanceList) {
        SdkInstance sdkInstance = new SdkInstance();
        sdkInstance.setDeviceId(DEVICE_ID);
        sdkInstance.setAccount(sdkAccount.getAccount());
        sdkInstance.setPassword(sdkAccount.getPassword());
        return serverInstanceList.stream().map(e -> {
            boolean socketConnected = ServerInstance.isSocketConnected(e, config.getTestSocketTimeoutMs());
            return new ServerInstanceClient(socketConnected, sdkInstance, e, config);
        }).collect(Collectors.toList());
    }

    @Override
    public void registerSdkInstance() {

    }

    @Override
    public void addServerListener(ServerListener serverListener) {
    }

    @Override
    public ReferenceCounted<List<ServerInstanceClient>> getServerListRef() {
        return serverListRef.open();
    }
}
