package com.github.dts.sdk.client;

import com.github.dts.sdk.conf.DtsSdkConfig;
import com.github.dts.sdk.util.DmlDTO;
import com.github.dts.sdk.util.JsonUtil;
import com.github.dts.sdk.util.MessageTypeEnum;
import com.github.dts.sdk.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerInstanceClient {
    private static final Logger log = LoggerFactory.getLogger(ServerInstanceClient.class);
    private final SdkInstance sdkInstance;
    private final ServerInstance serverInstance;
    private final DtsSdkConfig.ClusterConfig clusterConfig;
    /**
     * 网络是否可以连上
     */
    private final boolean socketConnected;
    private final Collection<URLConnection> connectionList = Collections.newSetFromMap(new IdentityHashMap<>());
    private final AtomicBoolean close = new AtomicBoolean(false);
    private volatile int discoveryCloseCount = 0;

    public ServerInstanceClient(boolean socketConnected,
                                SdkInstance sdkInstance,
                                ServerInstance serverInstance,
                                DtsSdkConfig.ClusterConfig clusterConfig) {
        this.socketConnected = socketConnected;
        this.sdkInstance = sdkInstance;
        this.serverInstance = serverInstance;
        this.clusterConfig = clusterConfig;
    }

    public void dump(DumpListener listener, long retrySleep, int maxRetry) {
        JsonUtil.ObjectReader objectReader = JsonUtil.objectReader();
        String basicAuth = "Basic " + Util.encodeBasicAuth(sdkInstance.getAccount(), sdkInstance.getPassword(), Charset.forName("UTF-8"));
        URL url;
        try {
            url = new URL(String.format("http://%s:%s%s/dts/sdk/subscriber",
                    serverInstance.getIp(), serverInstance.getPort(), clusterConfig.remoteContextPath()));
        } catch (MalformedURLException e) {
            Util.sneakyThrows(e);
            return;
        }
        URLConnection connection;
        try {
            connection = openConnection(url, basicAuth);
        } catch (IOException e) {
            Util.sneakyThrows(e);
            return;
        }

        while (!close.get()) {
            try {
                read(connection, objectReader, listener);
                return;
            } catch (IOException e) {
                if (close.get()) {
                    log.info("dump {} close {}", url, e.toString(), e);
                    return;
                }
                log.warn("dump {} fail {}", url, e.toString(), e);

                int retry = 0;
                boolean success = false;
                while (retry++ < maxRetry) {
                    if (close.get()) {
                        return;
                    }
                    if (needClose()) {
                        close();
                        return;
                    }
                    try {
                        connection = openConnection(url, basicAuth);
                        connection.getInputStream();
                        success = true;
                        discoveryCloseCount = 0;
                    } catch (IOException ignored) {
                        try {
                            Thread.sleep(retrySleep);
                        } catch (InterruptedException ex) {
                            Util.sneakyThrows(ex);
                        }
                    }
                }
                if (success) {
                    log.warn("dump reconnection success {}", url);
                } else {
                    if (close.get()) {
                        return;
                    }
                    if (needClose()) {
                        close();
                        return;
                    }
                    Util.sneakyThrows(e);
                    return;
                }
            }
        }
    }

    private boolean needClose() {
        return discoveryCloseCount > 0;
    }

    private URLConnection openConnection(URL url, String basicAuth) throws IOException {
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Authorization", basicAuth);
        connection.setRequestProperty("Authorization-fetch", "true");
        return connection;
    }

    private void read(URLConnection connection, JsonUtil.ObjectReader objectReader, DumpListener listener) throws IOException {
        synchronized (connectionList) {
            connectionList.add(connection);
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), Charset.forName("UTF-8")));
        Buffer buffer = new Buffer();
        while (!close.get()) {
            String s1;
            try {
                s1 = reader.readLine();
            } catch (Exception e) {
                disconnect(connection);
                throw e;
            }
            if (s1.startsWith("id:")) {
                buffer.id = Long.parseLong(s1.substring("id:".length()));
            } else if (s1.startsWith("event:")) {
                buffer.event = s1.substring("event:".length());
            } else if (s1.startsWith("data:")) {
                buffer.data = s1.substring("data:".length());
            } else if (s1.isEmpty() && !buffer.isEmpty()) {
                try {
                    MessageTypeEnum type = MessageTypeEnum.getByType(buffer.event);
                    if (type == MessageTypeEnum.ES_DML) {
                        DmlDTO dmlDTO = objectReader.readValue(buffer.data, DmlDTO.class);
                        listener.onEvent(buffer.id, dmlDTO);
                    }
                } finally {
                    buffer.clear();
                }
            }
        }
    }

    public String getAccount() {
        return serverInstance.getAccount();
    }

    public boolean isSocketConnected() {
        return socketConnected;
    }

    public ServerInstance getServerInstance() {
        return serverInstance;
    }

    public DtsSdkConfig.ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    public void discoveryClose() {
        discoveryCloseCount++;
    }

    private void disconnect(URLConnection urlConnection) {
        synchronized (connectionList) {
            connectionList.remove(urlConnection);
        }
        try {
            if (urlConnection instanceof HttpURLConnection) {
                ((HttpURLConnection) urlConnection).disconnect();
            } else {
                urlConnection.getInputStream().close();
            }
        } catch (Exception ignored) {
        }
    }

    public void close() {
        if (close.compareAndSet(false, true)) {
            log.info("server client close {}", serverInstance);
            ArrayList<URLConnection> list;
            synchronized (connectionList) {
                list = new ArrayList<>(connectionList);
                connectionList.clear();
            }
            for (URLConnection urlConnection : list) {
                try {
                    if (urlConnection instanceof HttpURLConnection) {
                        ((HttpURLConnection) urlConnection).disconnect();
                    } else {
                        urlConnection.getInputStream().close();
                    }
                } catch (Exception e) {
                    log.warn("Failed to close connection {}", e.toString());
                }
            }
        }
    }

    @Override
    public String toString() {
        return "ServerInstanceClient{" +
                "account=" + getAccount() +
                '}';
    }

    public interface DumpListener {
        void onEvent(Long messageId, Object data);
    }

    static class Buffer {
        Long id;
        String event;
        String data;

        void clear() {
            this.id = null;
            this.event = null;
            this.data = null;
        }

        public boolean isEmpty() {
            return id == null && event == null && data == null;
        }
    }
}
