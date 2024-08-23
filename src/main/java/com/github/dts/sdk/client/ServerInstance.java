package com.github.dts.sdk.client;

import com.github.dts.sdk.util.KeepaliveSocket;

import java.io.IOException;

public class ServerInstance {
    private String ip;
    private Integer port;
    private String deviceId;
    private String account;
    private String password;

    public static boolean isSocketConnected(ServerInstance instance, int timeout) {
        try (KeepaliveSocket socket = new KeepaliveSocket(instance.getIp(), instance.getPort())) {
            if (socket.isConnected(timeout)) {
                return true;
            }
        } catch (IOException ignored) {
        }
        return false;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
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

    @Override
    public String toString() {
        return "ServerInstance{" +
                "ip='" + ip + '\'' +
                ", port=" + port +
                ", deviceId='" + deviceId + '\'' +
                '}';
    }
}